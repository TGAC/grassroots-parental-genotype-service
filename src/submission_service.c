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
 * submission_service.c
 *
 *  Created on: 22 Oct 2018
 *      Author: billy
 */

#include <string.h>

#include "submission_service.h"
#include "parental_genotype_service.h"

#include "audit.h"
#include "streams.h"
#include "math_utils.h"
#include "string_utils.h"
#include "schema_keys.h"

/*
 * Static declarations
 */

static const char * const S_ID_S = "id";

static NamedParameterType S_SET_NAME = { "Name", PT_STRING };
static NamedParameterType S_SET_DATA = { "Data", PT_TABLE };


static const char *GetParentalGenotypeSubmissionServiceName (Service *service_p);

static const char *GetParentalGenotypeSubmissionServiceDesciption (Service *service_p);

static const char *GetParentalGenotypeSubmissionServiceInformationUri (Service *service_p);

static ParameterSet *GetParentalGenotypeSubmissionServiceParameters (Service *service_p, Resource *resource_p, UserDetails *user_p);

static void ReleaseParentalGenotypeSubmissionServiceParameters (Service *service_p, ParameterSet *params_p);

static ServiceJobSet *RunParentalGenotypeSubmissionService (Service *service_p, ParameterSet *param_set_p, UserDetails *user_p, ProvidersStateTable *providers_p);

static ParameterSet *IsResourceForParentalGenotypeSubmissionService (Service *service_p, Resource *resource_p, Handler *handler_p);

static bool CloseParentalGenotypeSubmissionService (Service *service_p);

static ServiceMetadata *GetParentalGenotypeSubmissionServiceMetadata (Service *service_p);



static bool AddChromosomes (json_t *doc_p, json_t *chromosomes_p);

static bool AddGeneticMappingPositions (json_t *doc_p, json_t *mappings_p);

static const char *AddParentRow (json_t *doc_p, json_t *genotypes_p, const char *key_s);

static bool AddGenotypesRow (json_t *doc_p, json_t *genotypes_p);


/*
 * API definitions
 */


Service *GetParentalGenotypeSubmissionService (void)
{
	Service *service_p = (Service *) AllocMemory (sizeof (Service));

	if (service_p)
		{
			ParentalGenotypeServiceData *data_p = AllocateParentalGenotypeServiceData ();

			if (data_p)
				{
					if (InitialiseService (service_p,
														 GetParentalGenotypeSubmissionServiceName,
														 GetParentalGenotypeSubmissionServiceDesciption,
														 GetParentalGenotypeSubmissionServiceInformationUri,
														 RunParentalGenotypeSubmissionService,
														 IsResourceForParentalGenotypeSubmissionService,
														 GetParentalGenotypeSubmissionServiceParameters,
														 ReleaseParentalGenotypeSubmissionServiceParameters,
														 CloseParentalGenotypeSubmissionService,
														 NULL,
														 false,
														 SY_SYNCHRONOUS,
														 (ServiceData *) data_p,
														 GetParentalGenotypeSubmissionServiceMetadata))
						{

							if (ConfigureParentalGenotypeService (data_p))
								{
									return service_p;
								}

						}		/* if (InitialiseService (.... */

					FreeParentalGenotypeServiceData (data_p);
				}

			FreeMemory (service_p);
		}		/* if (service_p) */

	return NULL;
}



static const char *GetParentalGenotypeSubmissionServiceName (Service * UNUSED_PARAM (service_p))
{
	return "ParentalGenotype submission service";
}


static const char *GetParentalGenotypeSubmissionServiceDesciption (Service * UNUSED_PARAM (service_p))
{
	return "A service to submit parental-cross genotype data";
}


static const char *GetParentalGenotypeSubmissionServiceInformationUri (Service * UNUSED_PARAM (service_p))
{
	return NULL;
}


static ParameterSet *GetParentalGenotypeSubmissionServiceParameters (Service *service_p, Resource * UNUSED_PARAM (resource_p), UserDetails * UNUSED_PARAM (user_p))
{
	ParameterSet *param_set_p = AllocateParameterSet ("Parental Genotype submission service parameters", "The parameters used for the Parental Genotype submission service");

	if (param_set_p)
		{
			ServiceData *data_p = service_p -> se_data_p;
			Parameter *param_p = NULL;
			SharedType def;
			ParameterGroup *group_p = CreateAndAddParameterGroupToParameterSet ("Field Trials", NULL, false, data_p, param_set_p);

			InitSharedType (&def);

			def.st_string_value_s = NULL;

			if ((param_p = EasyCreateAndAddParameterToParameterSet (data_p, param_set_p, group_p, S_SET_NAME.npt_type, S_SET_NAME.npt_name_s, "Name", "The name of the date set", def, PL_BASIC)) != NULL)
				{
					if ((param_p = EasyCreateAndAddParameterToParameterSet (data_p, param_set_p, group_p, S_SET_DATA.npt_type, S_SET_DATA.npt_name_s, "Data", "The parental-cross data", def, PL_BASIC)) != NULL)
						{
							return param_set_p;
						}
					else
						{
							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_SET_DATA.npt_name_s);
						}
				}
			else
				{
					PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_SET_NAME.npt_name_s);
				}

			FreeParameterSet (param_set_p);
		}
	else
		{
			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate %s ParameterSet", GetParentalGenotypeSubmissionServiceName (service_p));
		}

	return NULL;
}


static void ReleaseParentalGenotypeSubmissionServiceParameters (Service * UNUSED_PARAM (service_p), ParameterSet *params_p)
{
	FreeParameterSet (params_p);
}




static bool CloseParentalGenotypeSubmissionService (Service *service_p)
{
	bool success_flag = true;

	FreeParentalGenotypeServiceData ((ParentalGenotypeServiceData *) (service_p -> se_data_p));;

	return success_flag;
}



static ServiceJobSet *RunParentalGenotypeSubmissionService (Service *service_p, ParameterSet *param_set_p, UserDetails * UNUSED_PARAM (user_p), ProvidersStateTable * UNUSED_PARAM (providers_p))
{
	ParentalGenotypeServiceData *data_p = (ParentalGenotypeServiceData *) (service_p -> se_data_p);

	service_p -> se_jobs_p = AllocateSimpleServiceJobSet (service_p, NULL, "ParentalGenotype");

	if (service_p -> se_jobs_p)
		{
			ServiceJob *job_p = GetServiceJobFromServiceJobSet (service_p -> se_jobs_p, 0);

			LogParameterSet (param_set_p, job_p);

			SetServiceJobStatus (job_p, OS_FAILED_TO_START);

			if (param_set_p)
				{
					SharedType name_value;
					InitSharedType (&name_value);

					if (GetParameterValueFromParameterSet (param_set_p, S_SET_NAME.npt_name_s, &name_value, true))
						{
							const char *name_s = NULL;
							SharedType data_value;
							InitSharedType (&data_value);

							name_s = name_value.st_string_value_s;

							if (GetParameterValueFromParameterSet (param_set_p, S_SET_DATA.npt_name_s, &data_value, true))
								{
									const char *data_s = data_value.st_string_value_s;

									/*
									 * Has a spreadsheet been uploaded?
									 */
									if (! (IsStringEmpty (data_s)))
										{
											json_error_t e;
											json_t *data_json_p = NULL;

											/*
											 * The data could be either an array of json objects
											 * or a tabular string. so try it as json array first
											 */
											data_json_p = json_loads (data_s, 0, &e);

											if (data_json_p)
												{
													json_t *doc_p = json_object ();

													/*
														The organisation is:
														1 row = marker name
														2 row = chromosome / linkage group name
														3 row = genetic mapping position
														4 row = Parent A (always Paragon for this set)
														5 row = Parent B (always a Watkins landrace accession in format "Watkins 1190[0-9][0-9][0-9]"
														6 to last row = individuals of that population, progenies from the cross of Parent A with Parent B

														We abbreviate the population names from correctly: "Paragon x Watkins 1190[0-9][0-9][0-9]" to "ParW[0-9][0-9][0-9]".
														The code 1190xxx was the original number these lines were stored in the germplasm resource unit.
													 */
													if (json_is_array (data_json_p))
														{
															const size_t num_rows = json_array_size (data_json_p);

															/*
															 * There are 2 header rows, so the actual genotype data doesn't
															 * start until row 3
															 */
															if (num_rows >= 3)
																{
																	/*
																	 * Since the first row, the marker names, is used as the headers, the first entry should be
																	 * the chromosome / linkage group name
																	 */
																	size_t row_index = 0;
																	json_t *row_p = json_array_get (data_json_p, row_index);

																	if (AddChromosomes (doc_p, row_p))
																		{
																			/*
																			 * genetic mapping position
																			 */
																			row_p = json_array_get (data_json_p, ++ row_index);

																			if (AddGeneticMappingPositions (doc_p, row_p))
																				{
																					row_p = json_array_get (data_json_p, ++ row_index);
																					const char *parent_a_s = AddParentRow (doc_p, row_p, PGS_PARENT_A_S);

																					if (parent_a_s)
																						{
																							row_p = json_array_get (data_json_p, ++ row_index);
																							const char *parent_b_s = AddParentRow (doc_p, row_p, PGS_PARENT_B_S);

																							if (parent_b_s)
																								{
																									bool success_flag = true;

																									while ((++ row_index < num_rows) && success_flag)
																										{
																											row_p = json_array_get (data_json_p, row_index);

																											if (AddGenotypesRow (doc_p, row_p))
																												{
																													++ row_index;
																												}
																											else
																												{
																													success_flag = false;
																												}

																										}		/* while ((row_index < num_rows) && success_flag) */

																									if (success_flag)
																										{
																											/*
																											 * Save the document
																											 */
																										}

																								}		/* if (parent_b_s) */

																						}		/* if (parent_a_s) */

																				}		/* if (AddGeneticMappingPositions (doc_p, row_p)) */

																		}		/* if (AddChromosomes (doc_p, chromosomes_p)) */

																}		/* if (num_rows >= 3) */

														}		/* if (json_is_array (data_json_p)) */

													json_decref (data_json_p);
												}		/* if (data_json_p) */

										}		/* if (! (IsStringEmpty (data_s))) */

								}		/* if (GetParameterValueFromParameterSet (param_set_p, S_SET_DATA.npt_name_s, &data_value, true)) */

						}		/* if (GetParameterValueFromParameterSet (param_set_p, S_SET_NAME.npt_name_s, &name_value, true)) */

				}		/* if (param_set_p) */

			LogServiceJob (job_p);
		}		/* if (service_p -> se_jobs_p) */

	return service_p -> se_jobs_p;
}


static ServiceMetadata *GetParentalGenotypeSubmissionServiceMetadata (Service *service_p)
{
	const char *term_url_s = CONTEXT_PREFIX_EDAM_ONTOLOGY_S "topic_0625";
	SchemaTerm *category_p = AllocateSchemaTerm (term_url_s, "Genotype and phenotype",
		"The study of genetic constitution of a living entity, such as an individual, and organism, a cell and so on, "
		"typically with respect to a particular observable phenotypic traits, or resources concerning such traits, which "
		"might be an aspect of biochemistry, physiology, morphology, anatomy, development and so on.");

	if (category_p)
		{
			SchemaTerm *subcategory_p;

			term_url_s = CONTEXT_PREFIX_EDAM_ONTOLOGY_S "operation_0304";
			subcategory_p = AllocateSchemaTerm (term_url_s, "Query and retrieval", "Search or query a data resource and retrieve entries and / or annotation.");

			if (subcategory_p)
				{
					ServiceMetadata *metadata_p = AllocateServiceMetadata (category_p, subcategory_p);

					if (metadata_p)
						{
							SchemaTerm *input_p;

							term_url_s = CONTEXT_PREFIX_EDAM_ONTOLOGY_S "data_0968";
							input_p = AllocateSchemaTerm (term_url_s, "Keyword",
								"Boolean operators (AND, OR and NOT) and wildcard characters may be allowed. Keyword(s) or phrase(s) used (typically) for text-searching purposes.");

							if (input_p)
								{
									if (AddSchemaTermToServiceMetadataInput (metadata_p, input_p))
										{
											SchemaTerm *output_p;

											/* Place */
											term_url_s = CONTEXT_PREFIX_SCHEMA_ORG_S "Place";
											output_p = AllocateSchemaTerm (term_url_s, "Place", "Entities that have a somewhat fixed, physical extension.");

											if (output_p)
												{
													if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p))
														{
															/* Date */
															term_url_s = CONTEXT_PREFIX_SCHEMA_ORG_S "Date";
															output_p = AllocateSchemaTerm (term_url_s, "Date", "A date value in ISO 8601 date format.");

															if (output_p)
																{
																	if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p))
																		{
																			/* Pathogen */
																			term_url_s = CONTEXT_PREFIX_EXPERIMENTAL_FACTOR_ONTOLOGY_S "EFO_0000643";
																			output_p = AllocateSchemaTerm (term_url_s, "pathogen", "A biological agent that causes disease or illness to its host.");

																			if (output_p)
																				{
																					if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p))
																						{
																							/* Phenotype */
																							term_url_s = CONTEXT_PREFIX_EXPERIMENTAL_FACTOR_ONTOLOGY_S "EFO_0000651";
																							output_p = AllocateSchemaTerm (term_url_s, "phenotype", "The observable form taken by some character (or group of characters) "
																								"in an individual or an organism, excluding pathology and disease. The detectable outward manifestations of a specific genotype.");

																							if (output_p)
																								{
																									if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p))
																										{
																											/* Genotype */
																											term_url_s = CONTEXT_PREFIX_EXPERIMENTAL_FACTOR_ONTOLOGY_S "EFO_0000513";
																											output_p = AllocateSchemaTerm (term_url_s, "genotype", "Information, making the distinction between the actual physical material "
																												"(e.g. a cell) and the information about the genetic content (genotype).");

																											if (output_p)
																												{
																													if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p))
																														{
																															return metadata_p;
																														}		/* if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p)) */
																													else
																														{
																															PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add output term %s to service metadata", term_url_s);
																															FreeSchemaTerm (output_p);
																														}

																												}		/* if (output_p) */
																											else
																												{
																													PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate output term %s for service metadata", term_url_s);
																												}
																										}		/* if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p)) */
																									else
																										{
																											PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add output term %s to service metadata", term_url_s);
																											FreeSchemaTerm (output_p);
																										}

																								}		/* if (output_p) */
																							else
																								{
																									PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate output term %s for service metadata", term_url_s);
																								}

																						}		/* if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p)) */
																					else
																						{
																							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add output term %s to service metadata", term_url_s);
																							FreeSchemaTerm (output_p);
																						}

																				}		/* if (output_p) */
																			else
																				{
																					PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate output term %s for service metadata", term_url_s);
																				}

																		}		/* if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p)) */
																	else
																		{
																			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add output term %s to service metadata", term_url_s);
																			FreeSchemaTerm (output_p);
																		}

																}		/* if (output_p) */
															else
																{
																	PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate output term %s for service metadata", term_url_s);
																}


														}		/* if (AddSchemaTermToServiceMetadataOutput (metadata_p, output_p)) */
													else
														{
															PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add output term %s to service metadata", term_url_s);
															FreeSchemaTerm (output_p);
														}

												}		/* if (output_p) */
											else
												{
													PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate output term %s for service metadata", term_url_s);
												}

										}		/* if (AddSchemaTermToServiceMetadataInput (metadata_p, input_p)) */
									else
										{
											PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add input term %s to service metadata", term_url_s);
											FreeSchemaTerm (input_p);
										}

								}		/* if (input_p) */
							else
								{
									PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate input term %s for service metadata", term_url_s);
								}

						}		/* if (metadata_p) */
					else
						{
							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate service metadata");
						}

				}		/* if (subcategory_p) */
			else
				{
					PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate sub-category term %s for service metadata", term_url_s);
				}

		}		/* if (category_p) */
	else
		{
			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate category term %s for service metadata", term_url_s);
		}

	return NULL;
}


static bool AddChromosomes (json_t *doc_p, json_t *chromosomes_p)
{
	bool success_flag = true;
	void *iter_p = json_object_iter (chromosomes_p);

	while (iter_p && success_flag)
		{
	    const char *key_s = json_object_iter_key (iter_p);

	    if (strcmp (key_s, S_ID_S) != 0)
	    	{
	  	    const char *value_s = GetJSONString (chromosomes_p, key_s);

	  	    if (value_s)
	  	    	{
	  			    /* use key and value ... */
	  					json_t *marker_p = json_object ();

	  					if (marker_p)
	  						{
	  							if (json_object_set_new (doc_p, key_s, marker_p)  == 0)
	  								{
	  									if (!SetJSONString (marker_p, PGS_CHROMOSOME_S, value_s))
	  										{
	  											success_flag = false;
	  										}
	  								}
	  							else
	  								{
	  									success_flag = false;
	  									json_decref (marker_p);
	  								}

	  						}		/* if (marker_p) */
	  					else
	  						{
	  							success_flag = false;
	  						}

	  	    	}		/* if (value_s) */
	  	    else
	  				{
	  	    		success_flag = false;
	  				}

	    	}		/* if (strcmp (key_s, S_ID_S) != 0) */



	    iter_p = json_object_iter_next (chromosomes_p, iter_p);
		}

	return success_flag;
}



static bool AddGeneticMappingPositions (json_t *doc_p, json_t *mappings_p)
{
	bool success_flag = true;
	void *iter_p = json_object_iter (mappings_p);

	while (iter_p && success_flag)
		{
	    const char *key_s = json_object_iter_key (iter_p);

	    if (strcmp (key_s, S_ID_S) != 0)
	    	{
					const char *value_s = GetJSONString (mappings_p, key_s);

					if (value_s)
						{
							/* use key and value ... */
							json_t *marker_p = json_object_get (doc_p, key_s);

							if (marker_p)
								{
									if (!SetJSONString (marker_p, PGS_MAPPING_POSITION_S, value_s))
										{
											success_flag = false;
										}
								}		/* if (marker_p) */
							else
								{
									success_flag = false;
								}

						}		/* if (value_s) */
					else
						{
							success_flag = false;
						}

	    	}		/* if (strcmp (key_s, S_ID_S) != 0) */

	    iter_p = json_object_iter_next (mappings_p, iter_p);
		}

	return success_flag;
}


static const char *AddParentRow (json_t *doc_p, json_t *genotypes_p, const char *key_s)
{
	bool success_flag = false;
	const char *parent_s = GetJSONString (genotypes_p, S_ID_S);

	if (parent_s)
		{
			if (SetJSONString (doc_p, key_s, parent_s))
				{
					return parent_s;
				}
		}

	return NULL;
}


static bool AddGenotypesRow (json_t *doc_p, json_t *genotypes_p)
{
	bool success_flag = true;
	const char *accession_s = GetJSONString (genotypes_p, S_ID_S);

	if (accession_s)
		{
			void *iter_p = json_object_iter (genotypes_p);

			while (iter_p && success_flag)
				{
			    const char *key_s = json_object_iter_key (iter_p);

			    if (strcmp (key_s, S_ID_S) != 0)
			    	{
							const char *value_s = GetJSONString (genotypes_p, key_s);

							if (value_s)
								{
									/* use key and value ... */
									json_t *marker_p = json_object_get (doc_p, key_s);

									if (marker_p)
										{
											if (SetJSONString (marker_p, accession_s, value_s) != 0)
												{
													success_flag = false;
												}

										}		/* if (marker_p) */
									else
										{
											success_flag = false;
										}

								}		/* if (value_s) */
							else
								{
									success_flag = false;
								}

			    	}		/* if (strcmp (key_s, S_ID_S) == 0) else ... */

			    iter_p = json_object_iter_next (genotypes_p, iter_p);
				}

		}		/* if (accession_s) */
	else
		{
			success_flag = false;
		}


	return success_flag;
}



static ParameterSet *IsResourceForParentalGenotypeSubmissionService (Service * UNUSED_PARAM (service_p), Resource * UNUSED_PARAM (resource_p), Handler * UNUSED_PARAM (handler_p))
{
	return NULL;
}

