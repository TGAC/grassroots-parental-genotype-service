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

/**
 * @file
 * @brief
 */
/*
 * parental_genotype_service_data.h
 *
 *  Created on: 18 Nov 2018
 *      Author: tyrrells
 */

#ifndef PARENTAL_GENOTYPE_SERVICE_DATA_H_
#define PARENTAL_GENOTYPE_SERVICE_DATA_H_

#include "parental_genotype_service_library.h"
#include "jansson.h"

#include "service.h"
#include "mongodb_tool.h"



/**
 * The configuration data used by the Parental Genotype Service.
 *
 * @extends ServiceData
 */
typedef struct /*PARENTAL_GENOTYPE_SERVICE_LOCAL*/ ParentalGenotypeServiceData
{
	/** The base ServiceData. */
	ServiceData pgsd_base_data;


	/**
	 * @private
	 *
	 * The MongoTool to connect to the database where our data is stored.
	 */
	MongoTool *pgsd_mongo_p;


	/**
	 * @private
	 *
	 * The name of the database to use.
	 */
	const char *pgsd_database_s;


	/**
	 * @private
	 *
	 * The collection name of the population parentl-cross data use.
	 */
	const char *pgsd_populations_collection_s;

	/**
	 * @private
	 *
	 * The collection name of use.
	 */
	const char *pgsd_varieties_collection_s;


	json_t *pgsd_name_mappings_p;

} ParentalGenotypeServiceData;



#ifndef DOXYGEN_SHOULD_SKIP_THIS

#ifdef ALLOCATE_PARENTAL_GENOTYPE_SERVICE_TAGS
	#define PARENTAL_GENOTYPE_PREFIX PARENTAL_GENOTYPE_SERVICE_LOCAL
	#define PARENTAL_GENOTYPE_VAL(x)	= x
	#define PARENTAL_GENOTYPE_CONCAT_VAL(x,y) = x y
#else
	#define PARENTAL_GENOTYPE_PREFIX extern
	#define PARENTAL_GENOTYPE_VAL(x)
	#define PARENTAL_GENOTYPE_CONCAT_VAL(x,y) = x y
#endif

#endif 		/* #ifndef DOXYGEN_SHOULD_SKIP_THIS */


#ifdef __cplusplus
extern "C"
{
#endif

PARENTAL_GENOTYPE_SERVICE_LOCAL ParentalGenotypeServiceData *AllocateParentalGenotypeServiceData (void);


PARENTAL_GENOTYPE_SERVICE_LOCAL void FreeParentalGenotypeServiceData (ParentalGenotypeServiceData *data_p);


PARENTAL_GENOTYPE_SERVICE_LOCAL bool ConfigureParentalGenotypeService (ParentalGenotypeServiceData *data_p, GrassrootsServer *grassroots_p);

#ifdef __cplusplus
}
#endif


#endif /* PARENTAL_GENOTYPE_SERVICE_DATA_H_ */
