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

static bool GetParentalGenotypeSearchServiceParameterTypesForNamedParameters (struct Service *service_p, const char *param_name_s, ParameterType *pt_p);

static void ReleaseParentalGenotypeSearchServiceParameters (Service *service_p, ParameterSet *params_p);

static ServiceJobSet *RunParentalGenotypeSearchService (Service *service_p, ParameterSet *param_set_p, UserDetails *user_p, ProvidersStateTable *providers_p);

static ParameterSet *IsResourceForParentalGenotypeSearchService (Service *service_p, Resource *resource_p, Handler *handler_p);

static bool CloseParentalGenotypeSearchService (Service *service_p);

static ServiceMetadata *GetParentalGenotypeSearchServiceMetadata (Service *service_p);

static void DoSearch (ServiceJob *job_p, const char * const marker_s, const char * const population_s, bool full_record_flag, ParentalGenotypeServiceData *data_p);

static json_t *DoPopulationSearch (bson_t *query_p, const char * const population_s, const char * const marker_s, const char * const escaped_marker_s, ParentalGenotypeServiceData *data_p);

static json_t *GetForNamedMarker (const json_t *src_p, const char * const src_marker_s, const char * const dest_marker_s);

static bool CopyJSONString (const json_t *src_p, const char *src_key_s, json_t *dest_p, const char *dest_key_s);

static bool CopyJSONObject (const json_t *src_p, const char *src_key_s, json_t *dest_p, const char *dest_key_s);

static bool UnescapeAllKeys (json_t *src_p);


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
																 GetParentalGenotypeSearchServiceParameterTypesForNamedParameters,
																 ReleaseParentalGenotypeSearchServiceParameters,
																 CloseParentalGenotypeSearchService,
																 NULL,
																 false,
																 SY_SYNCHRONOUS,
																 (ServiceData *) data_p,
																 GetParentalGenotypeSearchServiceMetadata,
																 NULL))
						{
							if (ConfigureParentalGenotypeService (data_p))
								{
									return service_p;
								}

						}		/* if (InitialiseService (.... */
					else
						{
							FreeParentalGenotypeServiceData (data_p);
						}
				}

			if (service_p)
				{
					FreeService (service_p);
				}

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

			if ((param_p = EasyCreateAndAddParameterToParameterSet (data_p, param_set_p, group_p, S_MARKER.npt_type, S_MARKER.npt_name_s, "Marker", "The name of the marker to search for", def, PL_ALL)) != NULL)
				{
					if ((param_p = EasyCreateAndAddParameterToParameterSet (data_p, param_set_p, group_p, S_POPULATION.npt_type, S_POPULATION.npt_name_s, "Population", "The name of the population to search for", def, PL_ALL)) != NULL)
						{
							def.st_boolean_value = false;

							if ((param_p = EasyCreateAndAddParameterToParameterSet (data_p, param_set_p, group_p, S_FULL_RECORD.npt_type, S_FULL_RECORD.npt_name_s, "Full Records", "Return the full matching populations for marker search results", def, PL_ALL)) != NULL)
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


static bool GetParentalGenotypeSearchServiceParameterTypesForNamedParameters (struct Service *service_p, const char *param_name_s, ParameterType *pt_p)
{
	bool success_flag = true;

	if (strcmp (param_name_s, S_MARKER.npt_name_s) == 0)
		{
			*pt_p = S_MARKER.npt_type;
		}
	else if (strcmp (param_name_s, S_POPULATION.npt_name_s) == 0)
		{
			*pt_p = S_POPULATION.npt_type;
		}
	else if (strcmp (param_name_s, S_FULL_RECORD.npt_name_s) == 0)
		{
			*pt_p = S_FULL_RECORD.npt_type;
		}
	else
		{
			success_flag = false;
		}

	return success_flag;
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
			json_t *results_p = NULL;

			/*
			 * The marker name may contain full stops and although MongoDB 3.6+
			 * allows these, the current version of the mongo-c driver (1.13)
			 * does not, so we need to do the escaping ourselves
			 */
			char *escaped_marker_s = NULL;

			if (SearchAndReplaceInString (marker_s, &escaped_marker_s, ".", PGS_ESCAPED_DOT_S))
				{
					if (!IsStringEmpty (population_s))
						{
							if ((results_p = DoPopulationSearch (query_p, population_s, marker_s, escaped_marker_s, data_p)) != NULL)
								{
									/*
									 * Check whether we need to amalgamate the results
									 */
									size_t num_results = json_array_size (results_p);

									if (num_results > 0)
										{
											size_t i = 0;

											while (i < num_results)
												{
													json_t *i_entry_p = json_array_get (results_p, i);
													const char *i_name_s = GetJSONString (i_entry_p, PGS_POPULATION_NAME_S);

													if (i_name_s)
														{
															size_t j = i + 1;

															while (j < num_results)
																{
																	json_t *j_entry_p = json_array_get (results_p, j);
																	const char *j_name_s = GetJSONString (j_entry_p, PGS_POPULATION_NAME_S);
																	bool inc_flag = true;

																	if (j_name_s)
																		{
																			if (strcmp (i_name_s, j_name_s) == 0)
																				{
																					if (json_object_update_missing (i_entry_p, j_entry_p) == 0)
																						{
																							if (json_array_remove (results_p, j) == 0)
																								{
																									inc_flag = false;
																									-- num_results;
																								}		/* if (json_array_remove (results_p, j) == 0) */
																							else
																								{
																									PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, results_p, "Failed to remove index " SIZET_FMT, j);
																								}

																						}		/* if (json_object_update_missing (i_entry_p, j_entry_p) == 0) */
																					else
																						{
																							PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, i_entry_p, "json_object_update_missing failed");
																						}

																				}		/* if (strcmp (i_name_s, j_name_s) == 0) */

																		}		/* if (j_name_s) */
																	else
																		{
																			PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, j_entry_p, "Failed to get \"%s\"", PGS_POPULATION_NAME_S);
																		}

																	if (inc_flag)
																		{
																			++ j;
																		}
																}
														}
													else
														{
															PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, i_entry_p, "Failed to get \"%s\"", PGS_POPULATION_NAME_S);
														}

													++ i;
												}

										}		/* if (num_results > 0) */

									/*
									 * Since we've done a search for a population with no marker specified,
									 * we need to return all of the markers, i.e. the full record, so
									 * we need to make sure that the flag for this is set.
									 */
									full_record_flag = true;
								}
							else
								{
									PrintErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, "DoPopulationSearch failed for population \"%s\", marker \"%s\", escaped marker \"%s\"", population_s, marker_s ? marker_s: "", escaped_marker_s ? escaped_marker_s: "");
								}


						}		/* if (IsStringEmpty (population_s)) */
					else if (!IsStringEmpty (marker_s))
						{
							if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_populations_collection_s))
								{
									bson_t *child_p = BCON_NEW ("$exists", BCON_BOOL (true));

									if (child_p)
										{
											if (BSON_APPEND_DOCUMENT (query_p, escaped_marker_s ? escaped_marker_s : marker_s, child_p))
												{
													results_p = GetAllMongoResultsAsJSON (data_p -> pgsd_mongo_p, query_p, NULL);
												}

											bson_destroy (child_p);
										}
									else
										{

										}

								}		/* if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_accessions_collection_s)) */

						}		/* if (IsStringEmpty (marker_s)) */
					else
						{
							/*
							 * Nothing to do!
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
													/*
													 * We need to escape any keys that have [dot] in them
													 */

													if (UnescapeAllKeys (entry_p))
														{
															dest_record_p = GetResourceAsJSONByParts (PROTOCOL_INLINE_S, NULL, name_s, entry_p);
														}
													else
														{
															PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, entry_p, "UnescapeAllKeys failed");
														}
												}
											else
												{
													json_t *doc_p = json_object ();

													if (doc_p)
														{
															if (CopyJSONString (entry_p, PGS_PARENT_A_S, doc_p, NULL))
																{
																	if (CopyJSONString (entry_p, PGS_PARENT_B_S, doc_p, NULL))
																		{
																			json_t *marker_p = json_object_get (entry_p, escaped_marker_s ? escaped_marker_s : marker_s);

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

					if (escaped_marker_s)
						{
							FreeCopiedString (escaped_marker_s);
						}

				}		/* if (SearchAndReplaceInString (key_s, &escaped_marker_s, ".", PGS_DOT_S)) */


			bson_destroy (query_p);
		}		/* if (query_p) */

	SetServiceJobStatus (job_p, status);
}


static json_t *DoPopulationSearch (bson_t *query_p, const char * const population_s, const char * const marker_s, const char * const escaped_marker_s, ParentalGenotypeServiceData *data_p)
{
	json_t *results_p = NULL;

	if (BSON_APPEND_UTF8 (query_p, PGS_POPULATION_NAME_S, population_s))
		{
			if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_varieties_collection_s))
				{
					results_p = json_array ();

					if (results_p)
						{
							json_t *population_id_results_p = GetAllMongoResultsAsJSON (data_p -> pgsd_mongo_p, query_p, NULL);

							if (population_id_results_p)
								{
									const size_t num_results = json_array_size (population_id_results_p);
									size_t i = 0;
									bool success_flag = true;

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
																															 json_t *marker_only_p = GetForNamedMarker (population_p, escaped_marker_s ? escaped_marker_s : marker_s, marker_s);

																															 if (marker_only_p)
																																 {
																																	 if (json_array_append_new (results_p, marker_only_p) == 0)
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

									if (!success_flag)
										{
											json_decref (results_p);
											results_p = NULL;
										}

									json_decref (population_id_results_p);
								}		/* if (population_id_results_p) */

						}		/* if ((results_p = json_array ()) != NULL) */

				}		/* if (SetMongoToolCollection (data_p -> pgsd_mongo_p, data_p -> pgsd_accessions_collection_s)) */

		}		/* if (BSON_APPEND_UTF8 (query_p, PGS_POPULATION_NAME_S, population_s)) */
	else
		{
			PrintBSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, query_p, "Failed to add \"%s\": \"%s\" to query", PGS_POPULATION_NAME_S, population_s);
		}

	return results_p;
}



static json_t *GetForNamedMarker (const json_t *src_p, const char * const src_marker_s, const char * const dest_marker_s)
{
	json_t *dest_p = json_object ();

	if (dest_p)
		{
			if (CopyJSONString (src_p, PGS_POPULATION_NAME_S, dest_p, NULL))
				{
					if (CopyJSONString (src_p, PGS_PARENT_A_S, dest_p,  NULL))
						{
							if (CopyJSONString (src_p, PGS_PARENT_B_S, dest_p, NULL))
								{
									if (CopyJSONObject (src_p, src_marker_s, dest_p, dest_marker_s))
										{
											return dest_p;
										}

								}		/* if (CopyJSONString (src_p, dest_p, PGS_PARENT_B_S)) */

						}		/* if (CopyJSONString (src_p, dest_p, PGS_PARENT_A_S)) */

				}		/* if (CopyJSONString (src_p, dest_p, PGS_POPULATION_NAME_S)) */

			json_decref (dest_p);
		}		/* if (dest_p) */

	return NULL;
}


static bool CopyJSONString (const json_t *src_p, const char *src_key_s, json_t *dest_p, const char *dest_key_s)
{
	bool success_flag = false;
	const char *value_s = GetJSONString (src_p, src_key_s);

	if (value_s)
		{
			if (SetJSONString (dest_p, dest_key_s ? dest_key_s : src_key_s, value_s))
				{
					success_flag = true;
				}
		}

	return success_flag;
}


static bool CopyJSONObject (const json_t *src_p, const char *src_key_s, json_t *dest_p, const char *dest_key_s)
{
	bool success_flag = false;
	json_t *value_p = json_object_get (src_p, src_key_s);

	if (value_p)
		{
			if (json_object_set (dest_p, dest_key_s ? dest_key_s : src_key_s, value_p) == 0)
				{
					success_flag = true;
				}
		}

	return success_flag;
}


static bool UnescapeAllKeys (json_t *src_p)
{
	const char *key_s;
	json_t *value_p;
	void *tmp_p;

	json_object_foreach_safe (src_p, tmp_p, key_s, value_p)
		{
			if (strstr (key_s, PGS_ESCAPED_DOT_S))
				{
					char *unescaped_key_s = NULL;

					if (SearchAndReplaceInString (key_s, &unescaped_key_s, PGS_ESCAPED_DOT_S, "."))
						{
							if (json_object_set (src_p, unescaped_key_s, value_p) == 0)
								{
									if (json_object_del (src_p, key_s) != 0)
										{
											PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, src_p, "Failed to delete \"%s\" key", key_s);
											return false;
										}

								}		/* if (json_object_set (src_p, value_p) != 0) */
							else
								{
									PrintJSONToErrors (STM_LEVEL_SEVERE, __FILE__, __LINE__, src_p, "Failed to set \"%s\" key", unescaped_key_s);
									return false;
								}

							if (unescaped_key_s)
								{
									FreeCopiedString (unescaped_key_s);
								}

						}		/* if (SearchAndReplaceInString (key_s, &unescaped_key_s, PGS_ESCAPED_DOT_S, ".")) */

				}		/* if (strstr (key_s, PGS_ESCAPED_DOT_S)) */

		}		/* json_object_foreach_safe (src_p, key_s, value_p) */

	return true;
}

