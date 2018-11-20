/*
 * parental_genotype_service.h
 *
 *  Created on: 13 Jul 2018
 *      Author: billy
 */

#ifndef SERVICES_PARENTAL_GENOTYPE_SERVICE_INCLUDE_PARENTAL_GENOTYPE_SERVICE_H_
#define SERVICES_PARENTAL_GENOTYPE_SERVICE_INCLUDE_PARENTAL_GENOTYPE_SERVICE_H_


#include "parental_genotype_service_library.h"
#include "service.h"


#ifndef DOXYGEN_SHOULD_SKIP_THIS

#ifdef ALLOCATE_PARENTAL_GENOTYPE_SERVICE_TAGS
	#define PARENTAL_GENOTYPE_SERVICE_PREFIX PARENTAL_GENOTYPE_SERVICE_LOCAL
	#define PARENTAL_GENOTYPE_SERVICE_VAL(x)	= x
	#define PARENTAL_GENOTYPE_SERVICE_CONCAT_VAL(x,y)	= x y
#else
	#define PARENTAL_GENOTYPE_SERVICE_PREFIX extern
	#define PARENTAL_GENOTYPE_SERVICE_VAL(x)
	#define PARENTAL_GENOTYPE_SERVICE_CONCAT_VAL(x,y)
#endif

#endif 		/* #ifndef DOXYGEN_SHOULD_SKIP_THIS */



PARENTAL_GENOTYPE_SERVICE_PREFIX const char *PGS_CHROMOSOME_S PARENTAL_GENOTYPE_SERVICE_VAL ("chromosome");

PARENTAL_GENOTYPE_SERVICE_PREFIX const char *PGS_MAPPING_POSITION_S PARENTAL_GENOTYPE_SERVICE_VAL ("mapping_position");

PARENTAL_GENOTYPE_SERVICE_PREFIX const char *PGS_PARENT_A_S PARENTAL_GENOTYPE_SERVICE_VAL ("parent_a");

PARENTAL_GENOTYPE_SERVICE_PREFIX const char *PGS_PARENT_B_S PARENTAL_GENOTYPE_SERVICE_VAL ("parent_b");


#ifdef __cplusplus
extern "C"
{
#endif


/**
 * Get the Service available for running the DFW Field Trial Service.
 *
 * @param user_p The UserDetails for the user trying to access the services.
 * This can be <code>NULL</code>.
 * @return The ServicesArray containing the DFW Field Trial Service. or
 * <code>NULL</code> upon error.
 *
 * @ingroup dfw_field_trial_service
 */
PARENTAL_GENOTYPE_SERVICE_API ServicesArray *GetServices (UserDetails *user_p);


/**
 * Free the ServicesArray and its associated DFW Field Trial Service.
 *
 * @param services_p The ServicesArray to free.
 *
 * @ingroup dfw_field_trial_service
 */
PARENTAL_GENOTYPE_SERVICE_API void ReleaseServices (ServicesArray *services_p);



PARENTAL_GENOTYPE_SERVICE_LOCAL bool AddErrorMessage (ServiceJob *job_p, const json_t *value_p, const char *error_s, const int index);

#ifdef __cplusplus
}
#endif


#endif /* SERVICES_PARENTAL_GENOTYPE_SERVICE_INCLUDE_PARENTAL_GENOTYPE_SERVICE_H_ */
