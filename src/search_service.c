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
 * search_service.c
 *
 *  Created on: 24 Oct 2018
 *      Author: billy
 */

#include "search_service.h"
#include "parental_genotype_service.h"


#include "audit.h"
#include "streams.h"
#include "math_utils.h"
#include "string_utils.h"

/*
 * Static declarations
 */

static NamedParameterType S_MARKER = { "Marker", PT_KEYWORD };
static NamedParameterType S_POPULATION = { "Population", PT_KEYWORD };
static NamedParameterType S_FULL_RECORD = { "Return entire populations", PT_BOOLEAN };


static const char *GetParentalGenotypeSearchServiceName (Service *service_p);

static const char *GetParentalGenotypeSearchServiceDesciption (Service *service_p);

static const char *GetParentalGenotypeSearchServiceInformationUri (Service *service_p);

static ParameterSet *GetParentalGenotypeSearchServiceParameters (Service *service_p, Resource *resource_p, UserDetails *user_p);

static void ReleaseParentalGenotypeSearchServiceParameters (Service *service_p, ParameterSet *params_p);

static ServiceJobSet *RunParentalGenotypeSearchService (Service *service_p, ParameterSet *param_set_p, UserDetails *user_p, ProvidersStateTable *providers_p);

static ParameterSet *IsResourceForParentalGenotypeSearchService (Service *service_p, Resource *resource_p, Handler *handler_p);

static bool CloseParentalGenotypeSearchService (Service *service_p);

static ServiceMetadata *GetParentalGenotypeSearchServiceMetadata (Service *service_p);

static void DoSearch (ServiceJob *job_p, const char * const marker_s, const char * const population_s, bool full_record_flag, ParentalGenotypeServiceData *data_p);

static bool CopyJSONString (const json_t *src_p, json_t *dest_p, const char *key_s);

static json_t *GetForNamedMarker (const json_t *src_p, const char * const marker_s);


static bool CopyJSONObject (const json_t *src_p, json_t *dest_p, const char *key_s);


/*
 * API definitions
 */


Service *GetParentalGenotypeSearchService (void)
{
	Service *service_p = (Service *) AllocMemory (sizeof (Service));

	if (service_p)
		{
			ParentalGenotypeServiceData *data_p = AllocateParentalGenotypeServiceData ();

			if (data_p)
				{
					if (InitialiseService (service_p,
																 GetParentalGenotypeSearchServiceName,
																 GetParentalGenotypeSearchServiceDesciption,
																 GetParentalGenotypeSearchServiceInformationUri,
																 RunParentalGenotypeSearchService,
																 IsResourceForParentalGenotypeSearchService,
																 GetParentalGenotypeSearchServiceParameters,
																 ReleaseParentalGenotypeSearchServiceParameters,
																 CloseParentalGenotypeSearchService,
																 NULL,
																 false,
																 SY_SYNCHRONOUS,
																 (ServiceData *) data_p,
																 GetParentalGenotypeSearchServiceMetadata))
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



static const char *GetParentalGenotypeSearchServiceName (Service * UNUSED_PARAM (service_p))
{
	return "ParentalGenotype search service";
}


static const char *GetParentalGenotypeSearchServiceDesciption (Service * UNUSED_PARAM (service_p))
{
	return "A service to search field trial data";
}


static const char *GetParentalGenotypeSearchServiceInformationUri (Service * UNUSED_PARAM (service_p))
{
	return NULL;
}


static ParameterSet *GetParentalGenotypeSearchServiceParameters (Service *service_p, Resource * UNUSED_PARAM (resource_p), UserDetails * UNUSED_PARAM (user_p))
{
	ParameterSet *param_set_p = AllocateParameterSet ("ParentalGenotype search service parameters", "The parameters used for the ParentalGenotype search service");

	if (param_set_p)
		{
			ServiceData *data_p = service_p -> se_data_p;
			Parameter *param_p = NULL;
			SharedType def;
			ParameterGroup *group_p = NULL;

			InitSharedType (&def);

			def.st_string_value_s = NULL;

			if ((param_p = EasyCreateAndAddParameterToParameterSet (data_p, param_set_p, group_p, S_MARKER.npt_type, S_MARKER.npt_name_s, "Marker", "The name of the marker to search for", def, PL_BASIC)) != NULL)
				{
					if ((param_p = EasyCreateAndAddParameterToParameterSet (data_p, param_set_p, group_p, S_POPULATION.npt_type, S_POPULATION.npt_name_s, "Population", "The name of the population to search for", def, PL_BASIC)) != NULL)
						{
							def.st_boolean_value = false;

							if ((param_p = EasyCreateAndAddParameterToParameterSet (data_p, param_set_p, group_p, S_FULL_RECORD.npt_type, S_FULL_RECORD.npt_name_s, "Full Records", "Return the full matching populations for marker search results", def, PL_BASIC)) != NULL)
								{
									return param_set_p;
								}
							else
								{
									PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_FULL_RECORD.npt_name_s);
								}
						}
					else
						{
							PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_POPULATION.npt_name_s);
						}
				}
			else
				{
					PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to add %s parameter", S_MARKER.npt_name_s);
				}


			FreeParameterSet (param_set_p);
		}
	else
		{
			PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "Failed to allocate %s ParameterSet", GetParentalGenotypeSearchServiceName (service_p));
		}

	return NULL;
}


static void ReleaseParentalGenotypeSearchServiceParameters (Service * UNUSED_PARAM (service_p), ParameterSet *params_p)
{
	FreeParameterSet (params_p);
}


static bool CloseParentalGenotypeSearchService (Service *service_p)
{
	bool success_flag = true;

	FreeParentalGenotypeServiceData ((ParentalGenotypeServiceData *) (service_p -> se_data_p));;

	return success_flag;
}


static ServiceJobSet *RunParentalGenotypeSearchService (Service *service_p, ParameterSet *param_set_p, UserDetails * UNUSED_PARAM (user_p), ProvidersStateTable * UNUSED_PARAM (providers_p))
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
					SharedType marker_value;
					InitSharedType (&marker_value);

					if (GetParameterValueFromParameterSet (param_set_p, S_MARKER.npt_name_s, &marker_value, true))
						{
							SharedType population_value;
							InitSharedType (&population_value);

							if (GetParameterValueFromParameterSet (param_set_p, S_POPULATION.npt_name_s, &population_value, true))
								{
									SharedType full_records_value;
									InitSharedType (&full_records_value);

									full_records_value.st_boolean_value = false;
									GetParameterValueFromParameterSet (param_set_p, S_FULL_RECORD.npt_name_s, &full_records_value, true);

									DoSearch (job_p, marker_value.st_string_value_s, population_value.st_string_value_s, full_records_value.st_boolean_value, data_p);

								}		/* if (GetParameterValueFromParameterSet (param_set_p, S_MARKER.npt_name_s, &population_value, true)) */

						}		/* if (GetParameterValueFromParameterSet (param_set_p, S_MARKER.npt_name_s, &marker_value, true)) */

				}		/* if (param_set_p) */

#if DFW_FIELD_TRIAL_SERVICE_DEBUG >= STM_LEVEL_FINE
			PrintJSONToLog (STM_LEVEL_FINE, __FILE__, __LINE__, job_p -> sj_metadata_p, "metadata 3: ");
#endif

			LogServiceJob (job_p);
		}		/* if (service_p -> se_jobs_p) */

	return service_p -> se_jobs_p;
}


static ServiceMetadata *GetParentalGenotypeSearchServiceMetadata (Service * UNUSED_PARAM (service_p))
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

							FreeServiceMetadata (metadata_p);
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


static ParameterSet *IsResourceForParentalGenotypeSearchService (Service * UNUSED_PARAM (service_p), Resource * UNUSED_PARAM (resource_p), Handler * UNUSED_PARAM (handler_p))
{
	return NULL;
}


static void DoSearch (ServiceJob *job_p, const char * const marker_s, const char * const population_s, bool full_record_flag, ParentalGenotypeServiceData *data_p)
{
	OperationStatus status = OS_FAILED_TO_START;
	bson_t *query_p = bson_new ();

	if (query_p)
		{
			bool success_flag = true;
			json_t *results_p = NULL;

			if (!IsStringEmpty (population_s))
				{
					if (BSON_APPEND_UTF8 (query_p, PGS_POPULATION_NAME_S, population_s))
						{
							if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_varieties_collection_s))
								{
									if ((results_p = json_array ()) != NULL)
										{
											json_t *population_id_results_p = GetAllMongoResultsAsJSON (data_p -> pgsd_mongo_p, query_p, NULL);

											if (population_id_results_p)
												{
													const size_t num_results = json_array_size (population_id_results_p);
													size_t i = 0;

													while ((i < num_results) && success_flag)
														{
															const json_t *entry_p = json_array_get (population_id_results_p, i);
															const json_t *population_ids_p = json_object_get (entry_p, PGS_VARIETY_IDS_S);

															if (population_ids_p)
																{
																	if (json_is_array (population_ids_p))
																		{
																			const size_t num_ids = json_array_size (population_ids_p);
																			size_t j = 0;

																			if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_populations_collection_s))
																				{
																					while ((j < num_ids) && success_flag)
																						{
																							const json_t *population_id_p = json_array_get (population_ids_p, j);
																							bson_oid_t population_oid;
																							bool added_flag = false;

																							if (GetIdFromJSONKeyValuePair (population_id_p, &population_oid))
																								{
																									/*
																									 * Now we have the id we can get the population
																									 */
																									 bson_t *pop_query_p = bson_new ();

																									 if (pop_query_p)
																										 {
																											 if (BSON_APPEND_OID (pop_query_p, "_id", &population_oid))
																												 {
																													 json_t *populations_p = GetAllMongoResultsAsJSON (data_p -> pgsd_mongo_p, pop_query_p, NULL);

																													 if (populations_p)
																														 {
																															 if ((json_is_array (populations_p)) && (json_array_size (populations_p) == 1))
																																 {
																																	 json_t *population_p = json_array_get (populations_p, 0);

																																	 if (IsStringEmpty (marker_s))
																																		 {
																																			 /*
																																			  * Add all of the markers
																																			  */
																																			 if (json_array_append (results_p, population_p) == 0)
																																				 {
																																					 added_flag = true;
																																				 }
																																		 }
																																	 else
																																		 {
																																			 /*
																																			  * Just add our marker
																																			  */
																																			 json_t *marker_only_p = GetForNamedMarker (population_p, marker_s);

																																			 if (marker_only_p)
																																				 {
																																					 if (json_array_append (results_p, marker_only_p) == 0)
																																						 {
																																							 added_flag = true;
																																						 }
																																					 else
																																						 {
																																							 json_decref (marker_only_p);
																																						 }
																																				 }

																																		 }

																																 }		/* if ((json_is_array (populations_p)) && (json_array_size (populations_p) == 1)) */

																															 json_decref (populations_p);
																														 }		/* if (populations_p) */

																												 }		/* if (BSON_APPEND_OID (pop_query_p, "_id", &population_oid)) */

																											 bson_destroy (pop_query_p);
																										 }		/* if (pop_query_p) */

																								}		/* if (GetIdFromJSONKeyValuePair (population_id_p, &population_oid)) */
																							else
																								{
																									success_flag = false;
																								}

																							if (added_flag)
																								{
																									++ j;
																								}
																							else
																								{
																									success_flag = false;
																								}
																						}		/* while ((j < num_ids) && success_flag) */

																				}		/* if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_populations_collection_s)) */

																		}		/* if (json_is_array (population_ids_p)) */

																}		/* if (population_ids_p) */

															++ i;
														}		/* while ((i < num_results) && success_flag) */

													if (success_flag)
														{
															/*
															 * Since we've done a search for a population with no marker specified,
															 * we need to return all of the markers, i.e. the full record, so
															 * we need to make sure that the flag for this is set.
															 */
															full_record_flag = true;
														}
													else
														{
															json_decref (results_p);
															results_p = NULL;
														}

												}		/* if (population_id_results_p) */

										}		/* if ((results_p = json_array ()) != NULL) */

								}		/* if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_accessions_collection_s)) */

						}		/* if (BSON_APPEND_UTF8 (query_p, PGS_POPULATION_NAME_S, population_s)) */
					else
						{
							PrintBSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, query_p, "Failed to add \"%s\": \"%s\" to query", PGS_POPULATION_NAME_S, population_s);
						}

				}		/* if (IsStringEmpty (population_s)) */
			else if (!IsStringEmpty (marker_s))
				{
					if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_populations_collection_s))
						{
							if (!IsStringEmpty (marker_s))
								{
									bson_t *child_p = BCON_NEW ("$exists", BCON_BOOL (true));

									if (child_p)
										{
											if (BSON_APPEND_DOCUMENT (query_p, marker_s, child_p))
												{
													results_p = GetAllMongoResultsAsJSON (data_p -> pgsd_mongo_p, query_p, NULL);
												}

											bson_destroy (child_p);
										}
									else
										{
											success_flag = false;
										}
								}		/* if (!IsStringEmpty (marker_s)) */

						}		/* if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_accessions_collection_s)) */


				}		/* if (IsStringEmpty (marker_s)) */
			else
				{
					/*
					 * Nothng to do!
					 */
				}

			if (results_p)
				{
					if (json_is_array (results_p))
						{
							size_t i = 0;
							size_t num_added = 0;
							const size_t num_results = json_array_size (results_p);

							for (i = 0; i < num_results; ++ i)
								{
									json_t *entry_p = json_array_get (results_p, i);
									const char *name_s = GetJSONString (entry_p, PGS_POPULATION_NAME_S);
									json_t *dest_record_p = NULL;

									json_object_del (entry_p, MONGO_ID_S);


									if (full_record_flag)
										{
											dest_record_p = GetResourceAsJSONByParts (PROTOCOL_INLINE_S, NULL, name_s, entry_p);
										}
									else
										{
											json_t *doc_p = json_object ();

											if (doc_p)
												{
													if (CopyJSONString (entry_p, doc_p, PGS_PARENT_A_S))
														{
															if (CopyJSONString (entry_p, doc_p, PGS_PARENT_B_S))
																{
																	json_t *marker_p = json_object_get (entry_p, marker_s);

																	if (marker_p)
																		{
																			if (json_object_set (doc_p, marker_s, marker_p) == 0)
																				{
																					dest_record_p = GetResourceAsJSONByParts (PROTOCOL_INLINE_S, NULL, name_s, doc_p);
																				}
																		}

																}		/* if (CopyJSONString (entry_p, doc_p, PGS_PARENT_B_S)) */
															else
																{
																	PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, entry_p, "Failed to copy %s", PGS_PARENT_B_S);
																}

														}		/* if (CopyJSONString (entry_p, doc_p, PGS_PARENT_A_S)) */
													else
														{
															PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, entry_p, "Failed to copy %s", PGS_PARENT_A_S);
														}

													json_decref (doc_p);
												}		/* if (doc_p) */

										}

									if (dest_record_p)
										{
											if (AddResultToServiceJob (job_p, dest_record_p))
												{
													++ num_added;
												}
											else
												{
													json_decref (dest_record_p);
												}

										}		/* if (dest_record_p) */

									if (population_s)
										{

										}
								}

							if (num_added == num_results)
								{
									status = OS_SUCCEEDED;
								}
							else if (num_added > 0)
								{
									status = OS_PARTIALLY_SUCCEEDED;
								}
							else
								{
									status = OS_FAILED;
								}

						}		/* if (json_is_array (results_p)) */

					json_decref (results_p);
				}		/* if (results_p) */

			bson_destroy (query_p);
		}		/* if (query_p) */

	SetServiceJobStatus (job_p, status);
}



static json_t *GetForNamedMarker (const json_t *src_p, const char * const marker_s)
{
	json_t *dest_p = json_object ();

	if (dest_p)
		{
			if (CopyJSONString (src_p, dest_p, PGS_POPULATION_NAME_S))
				{
					if (CopyJSONString (src_p, dest_p, PGS_PARENT_A_S))
						{
							if (CopyJSONString (src_p, dest_p, PGS_PARENT_B_S))
								{
									if (CopyJSONObject (src_p, dest_p, marker_s))
										{
											return dest_p;
										}		/* if (CopyJSONObject (src_p, dest_p, marker_s)) */

								}		/* if (CopyJSONString (src_p, dest_p, PGS_PARENT_B_S)) */

						}		/* if (CopyJSONString (src_p, dest_p, PGS_PARENT_A_S)) */

				}		/* if (CopyJSONString (src_p, dest_p, PGS_POPULATION_NAME_S)) */

			json_decref (dest_p);
		}		/* if (dest_p) */

	return NULL;
}


static bool CopyJSONString (const json_t *src_p, json_t *dest_p, const char *key_s)
{
	bool success_flag = false;
	const char *value_s = GetJSONString (src_p, key_s);

	if (value_s)
		{
			if (SetJSONString (dest_p, key_s, value_s))
				{
					success_flag = true;
				}
		}

	return success_flag;
}


static bool CopyJSONObject (const json_t *src_p, json_t *dest_p, const char *key_s)
{
	bool success_flag = false;
	json_t *value_p = json_object_get (src_p, key_s);

	if (value_p)
		{
			if (json_object_set (dest_p, key_s, value_p) == 0)
				{
					success_flag = true;
				}
		}

	return success_flag;
}
