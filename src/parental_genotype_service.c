/*
 ** Copyright 2014-2016 The Earlham Institute
 **
 ** Licensed under the Apache License, Version 2.0 (the "License");
 ** you may not use this file except in compliance with the License.
 ** You may obtain a copy of the License at
 **
 **     http://www.apache.org/licenses/LICENSE-2.0
 **
 ** Unless required by applicable law or agreed to in writing, software
 ** distributed under the License is distributed on an "AS IS" BASIS,
 ** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ** See the License for the specific language governing permissions and
 ** limitations under the License.
 */
#include <string.h>

#include "jansson.h"

#define ALLOCATE_PARENTAL_GENOTYPE_SERVICE_TAGS (1)
#include "parental_genotype_service.h"
#include "parental_genotype_service_data.h"
#include "memory_allocations.h"
#include "parameter.h"
#include "service_job.h"
#include "string_utils.h"
#include "json_tools.h"
#include "grassroots_server.h"
#include "string_linked_list.h"
#include "math_utils.h"
#include "search_options.h"
#include "time_util.h"
#include "io_utils.h"
#include "audit.h"

#include "search_service.h"
#include "submission_service.h"


#ifdef _DEBUG
#define PARENTAL_GENOTYPE_SERVICE_DEBUG	(STM_LEVEL_FINER)
#else
#define PARENTAL_GENOTYPE_SERVICE_DEBUG	(STM_LEVEL_NONE)
#endif



/*
 * STATIC PROTOTYPES
 */


/*
 * API FUNCTIONS
 */


ServicesArray *GetServices (UserDetails *user_p, GrassrootsServer *grassroots_p)
{
	uint32 num_services = 0;
	Service *submission_service_p = GetParentalGenotypeSubmissionService (grassroots_p);
	Service *search_service_p = GetParentalGenotypeSearchService (grassroots_p);

	if (submission_service_p)
		{
			++ num_services;
		}

	if (search_service_p)
		{
			++ num_services;
		}


	if (num_services)
		{
			ServicesArray *services_p = AllocateServicesArray (num_services);

			if (services_p)
				{
					num_services = 0;

					if (submission_service_p)
						{
							* (services_p -> sa_services_pp) = submission_service_p;
							++ num_services;
						}


					if (search_service_p)
						{
							* ((services_p -> sa_services_pp) + num_services) = search_service_p;
						}


					return services_p;
				}
		}


	if (submission_service_p)
		{
			FreeService (submission_service_p);
		}

	if (search_service_p)
		{
			FreeService (submission_service_p);
		}


	return NULL;
}


void ReleaseServices (ServicesArray *services_p)
{
	FreeServicesArray (services_p);
}




bool AddErrorMessage (ServiceJob *job_p, const json_t *value_p, const char *error_s, const int index)
{
	char *dump_s = json_dumps (value_p, JSON_INDENT (2) | JSON_PRESERVE_ORDER);
	const char *id_s = GetJSONString (value_p, "id");
	bool added_error_flag = false;


	if (id_s)
		{
			added_error_flag = AddErrorToServiceJob (job_p, id_s, error_s);
		}
	else
		{
			char *index_s = GetIntAsString (index);

			if (index_s)
				{
					char *row_s = ConcatenateStrings ("row ", index_s);

					if (row_s)
						{
							added_error_flag = AddErrorToServiceJob (job_p, row_s, error_s);

							FreeCopiedString (row_s);
						}

					FreeCopiedString (index_s);
				}

		}

	if (!added_error_flag)
		{
			PrintErrors (STM_LEVEL_WARNING, __FILE__, __LINE__, "failed to add %s to client feedback messsage", error_s);
		}


	if (dump_s)
		{
			PrintErrors (STM_LEVEL_WARNING, __FILE__, __LINE__, "Failed to import \"%s\": error=%s", dump_s, error_s);
			free (dump_s);
		}
	else
		{
			PrintErrors (STM_LEVEL_WARNING, __FILE__, __LINE__, "Failed to import error=%s", dump_s, error_s);
		}

	return added_error_flag;
}

