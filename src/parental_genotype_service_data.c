/*
** Copyright 2014-2018 The Earlham Institute
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
/*
 * parental_genotype_service_service_data.c
 *
 *  Created on: 18 Nov 2018
 *      Author: billy
 */

#define ALLOCATE_PARENTAL_GENOTYPE_SERVICE_TAGS (1)
#include "parental_genotype_service_data.h"

#include "streams.h"
#include "string_utils.h"


ParentalGenotypeServiceData *AllocateParentalGenotypeServiceData  (void)
{
	MongoTool *tool_p = AllocateMongoTool (NULL);

	if (tool_p)
		{
			ParentalGenotypeServiceData *data_p = (ParentalGenotypeServiceData *) AllocMemory (sizeof (ParentalGenotypeServiceData));

			if (data_p)
				{
					data_p -> pgsd_mongo_p = tool_p;
					data_p -> pgsd_database_s = NULL;
					data_p -> pgsd_accessions_collection_s = NULL;
					data_p -> pgsd_markers_collection_s = NULL;

					return data_p;
				}

			FreeMongoTool (tool_p);
		}		/* if (tool_p) */

	return NULL;
}


void FreeParentalGenotypeServiceData (ParentalGenotypeServiceData *data_p)
{
	if (data_p -> pgsd_mongo_p)
		{
			FreeMongoTool (data_p -> pgsd_mongo_p);
		}

	FreeMemory (data_p);
}


bool ConfigureParentalGenotypeService (ParentalGenotypeServiceData *data_p)
{
	bool success_flag = false;
	const json_t *service_config_p = data_p -> pgsd_base_data.sd_config_p;

	data_p -> pgsd_database_s = GetJSONString (service_config_p, "database");

	if (data_p -> pgsd_database_s)
		{
			if ((data_p -> pgsd_accessions_collection_s = GetJSONString (service_config_p, "accessions_collection")) != NULL)
				{
					if ((data_p -> pgsd_markers_collection_s = GetJSONString (service_config_p, "markers_collection")) != NULL)
						{
							if (SetMongoToolDatabase (data_p -> pgsd_mongo_p, data_p -> pgsd_database_s))
								{
									success_flag = true;
								}
						} 	/* if ((data_p -> pgsd_markers_collection_s = GetJSONString (service_config_p, "markers_collection")) != NULL) */

				}		/* if ((data_p -> pgsd_accessions_collection_s = GetJSONString (service_config_p, "accessions_collection")) != NULL) */

		}		/* if (data_p -> psd_database_s) */

	return success_flag;
}


