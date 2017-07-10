using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Web;
using Newtonsoft.Json.Linq;
using Sitecore.Abstractions;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Azure;
using Sitecore.ContentSearch.Azure.Config;
using Sitecore.ContentSearch.Azure.FieldMaps;
using Sitecore.ContentSearch.Azure.Query;
using Sitecore.ContentSearch.Azure.Schema;
using Sitecore.ContentSearch.Azure.Utils;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Linq;
using Sitecore.ContentSearch.Linq.Common;
using Sitecore.ContentSearch.Linq.Methods;
using Sitecore.ContentSearch.Linq.Nodes;
using Sitecore.ContentSearch.Pipelines.GetFacets;
using Sitecore.ContentSearch.Pipelines.ProcessFacets;
using Sitecore.ContentSearch.Utilities;
using Sitecore.Diagnostics;

namespace Sitecore.Support.ContentSearch.Azure.Query
{
    public class LinqToCloudIndex<TItem> : Sitecore.Support.ContentSearch.Azure.Query.CloudIndex<TItem>
    {
        private readonly CloudSearchSearchContext context;

        private readonly ICorePipeline pipeline;

        public LinqToCloudIndex([NotNull] CloudSearchSearchContext context, IExecutionContext executionContext)
            : this(context, new[] { executionContext })
        {
        }

        public LinqToCloudIndex([NotNull] CloudSearchSearchContext context, IExecutionContext[] executionContexts)
            : base(
                new CloudIndexParameters(
                    context.Index.Configuration.IndexFieldStorageValueFormatter,
                    context.Index.Configuration.VirtualFields,
                    context.Index.FieldNameTranslator,
                    typeof(TItem),
                    false,
                    executionContexts,
                    context.Index.Schema))
        {
            Assert.ArgumentNotNull(context, "context");
            this.pipeline = context.Index.Locator.GetInstance<ICorePipeline>();
            this.context = context;
        }

        public override TResult Execute<TResult>(CloudQuery query)
        {
            var selectMethod = GetSelectMethod(query);
            int totalDoc, countDoc;
            Dictionary<string, object> facetResult;
            if (typeof(TResult).IsGenericType && typeof(TResult).GetGenericTypeDefinition() == typeof(SearchResults<>))
            {
                var documentType = typeof(TResult).GetGenericArguments()[0];
                var results = this.Execute(query, out countDoc, out totalDoc, out facetResult);

                var cloudSearchResultsType = typeof(CloudSearchResults<>);
                var cloudSearchResultsGenericType = cloudSearchResultsType.MakeGenericType(documentType);

                var applyScalarMethodsMethod = this.GetType().GetMethod("ApplyScalarMethods", BindingFlags.Instance | BindingFlags.NonPublic);
                var applyScalarMethodsGenericMethod = applyScalarMethodsMethod.MakeGenericMethod(typeof(TResult), documentType);

                // Execute query methods
                var processedResults = Activator.CreateInstance(cloudSearchResultsGenericType, this.context, results, selectMethod, countDoc, facetResult, this.Parameters, query.VirtualFieldProcessors);

                return (TResult)applyScalarMethodsGenericMethod.Invoke(this, new[] { query, processedResults, totalDoc });
            }
            else
            {
                var valueList = this.Execute(query, out countDoc, out totalDoc, out facetResult);
                var processedResults = new CloudSearchResults<TItem>(this.context, valueList, selectMethod, countDoc, facetResult, this.Parameters, query.VirtualFieldProcessors);

                return this.ApplyScalarMethods<TResult, TItem>(query, processedResults, totalDoc);
            }
        }

        public override IEnumerable<TElement> FindElements<TElement>(CloudQuery query)
        {
            if (EnumerableLinq.ShouldExecuteEnumerableLinqQuery(query))
            {
                return EnumerableLinq.ExecuteEnumerableLinqQuery<IEnumerable<TElement>>(query);
            }

            int totalDoc, countDoc;
            Dictionary<string, object> facetResult;
            var valueList = this.Execute(query, out countDoc, out totalDoc, out facetResult);
            var selectMethod = GetSelectMethod(query);
            var processedResults = new CloudSearchResults<TElement>(this.context, valueList, selectMethod, countDoc, facetResult, this.Parameters, query.VirtualFieldProcessors);
            return processedResults.GetSearchResults();
        }

        internal List<Dictionary<string, object>> Execute(CloudQuery query, out int countDoc, out int totalDoc, out Dictionary<string, object> facetResult)
        {
            countDoc = 0;
            totalDoc = 0;
            facetResult = new Dictionary<string, object>();
            if (!string.IsNullOrEmpty(query.Expression) || query.Methods.Count > 0)
            {
                var searchIndex = this.context.Index as CloudSearchProviderIndex;
                if (searchIndex == null)
                {
                    return new List<Dictionary<string, object>>();
                }

                if (query.Expression.Contains(CloudQueryBuilder.Search.SearchForNothing))
                {
                    this.LogSearchQuery(query.Expression, this.context.Index);
                    return new List<Dictionary<string, object>>();
                }

                // Finalize the query expression
                var expression = this.OptimizeQueryExpression(query, searchIndex) + "&$count=true";
                // append $count=true to find out total found document, but has performance impact;

                try
                {
                    this.LogSearchQuery(expression, this.context.Index);
                    var results = searchIndex.SearchService.Search(expression);

                    if (string.IsNullOrEmpty(results))
                    {
                        return new List<Dictionary<string, object>>();
                    }

                    // TODO Replace this handmade deserialization with SearchResultsDeserializer.
                    var valueList = JObject.Parse(results).SelectToken("value").Select(x => JsonHelper.Deserialize(x.ToString()) as Dictionary<string, object>).ToList();
                    if (valueList.Count != 0)
                    {
                        totalDoc = JObject.Parse(results)["@odata.count"].ToObject<int>();
                        countDoc = totalDoc;
                        var skipFields = query.Methods.Where(m => m.MethodType == QueryMethodType.Skip).Select(m => (SkipMethod)m).ToList();
                        if (skipFields.Any())
                        {
                            var start = skipFields.Sum(skipMethod => skipMethod.Count);
                            countDoc = countDoc - start;
                        }

                        // If take method is defined, total doc will based on that
                        var takeFields = query.Methods.Where(m => m.MethodType == QueryMethodType.Take).Select(m => (TakeMethod)m).ToList();
                        if (takeFields.Any())
                        {
                            countDoc = takeFields.Sum(takeMethod => takeMethod.Count);
                            if (valueList.Count < countDoc)
                            {
                                countDoc = valueList.Count;
                            }
                        }

                        var facetData = JObject.Parse(results).GetValue("@search.facets");
                        if (facetData != null)
                        {
                            facetResult = JObject.Parse(results).GetValue("@search.facets").ToObject<Dictionary<string, object>>();
                        }

                        return valueList;
                    }
                }
                catch (Exception ex)
                {
                    SearchLog.Log.Error(
                        string.Format("Azure Search Error [Index={0}] ERROR:{1} Search expression:{2}", searchIndex.Name, ex.Message, query.Expression));

                    throw;
                }
            }

            return new List<Dictionary<string, object>>();
        }

        /// <summary>
        /// Logs a serialized native query.
        /// </summary>
        /// <param name="expression">A native search query.</param>
        /// <param name="index">An index against which the query is executed.</param>
        protected void LogSearchQuery(string expression, ISearchIndex index)
        {
            SearchLog.Log.Info(string.Format("AzureSearch Query [{0}]: {1}", this.context.Index.Name, expression));
        }

        /// <summary>
        /// Gets the select method.
        /// </summary>
        /// <param name="compositeQuery">
        /// The composite query.
        /// </param>
        /// <returns>
        /// Select method.
        /// </returns>
        private static SelectMethod GetSelectMethod(CloudQuery compositeQuery)
        {
            var selectMethods =
                compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.Select)
                    .Select(m => (SelectMethod)m)
                    .ToList();

            return selectMethods.Count() == 1 ? selectMethods[0] : null;
        }

        private TResult ApplyScalarMethods<TResult, TDocument>(CloudQuery query, CloudSearchResults<TDocument> processedResults, int? totalCount)
        {
            var method = query.Methods.First();

            object result;

            switch (method.MethodType)
            {
                case QueryMethodType.All:
                    // Predicate have been applied to the where clause.
                    result = true;
                    break;
                case QueryMethodType.Any:
                    result = processedResults.Any();
                    break;
                case QueryMethodType.Count:
                    result = processedResults.Count();
                    break;
                case QueryMethodType.ElementAt:
                    result = ((ElementAtMethod)method).AllowDefaultValue
                                 ? processedResults.ElementAtOrDefault(((ElementAtMethod)method).Index)
                                 : processedResults.ElementAt(((ElementAtMethod)method).Index);
                    break;
                case QueryMethodType.GetResults:
                    var resultList = processedResults.GetSearchHits();
                    var facets = this.FormatFacetResults(processedResults.GetFacets(), query.FacetQueries);
                    var count = totalCount ?? unchecked((int)processedResults.Count());
                    result = ReflectionUtility.CreateInstance(typeof(TResult), resultList, count, facets);
                    // Create instance of SearchResults<TDocument>
                    break;
                case QueryMethodType.First:
                    result = ((FirstMethod)method).AllowDefaultValue
                                 ? processedResults.FirstOrDefault()
                                 : processedResults.First();
                    break;
                case QueryMethodType.GetFacets:
                    result = this.FormatFacetResults(processedResults.GetFacets(), query.FacetQueries);
                    break;
                case QueryMethodType.Last:
                    result = ((LastMethod)method).AllowDefaultValue
                                 ? processedResults.LastOrDefault()
                                 : processedResults.Last();
                    break;
                case QueryMethodType.Single:
                    result = ((SingleMethod)method).AllowDefaultValue
                                 ? processedResults.SingleOrDefault()
                                 : processedResults.Single();
                    break;
                default:
                    throw new InvalidOperationException("Invalid query method");
            }

            return (TResult)System.Convert.ChangeType(result, typeof(TResult));
        }

        private FacetResults FormatFacetResults(
            Dictionary<string, ICollection<KeyValuePair<string, int>>> facetResults,
            List<FacetQuery> facetQueries)
        {
            if (facetResults == null) return null;

            var fieldTranslator = this.context.Index.FieldNameTranslator as CloudFieldNameTranslator;
            var processedFacets = ProcessFacetsPipeline.Run(
                this.pipeline,
                new ProcessFacetsArgs(facetResults, facetQueries, facetQueries, this.context.Index.Configuration.VirtualFieldProcessors, fieldTranslator));

            var facetFormattedResults = new FacetResults();

            if (fieldTranslator == null)
            {
                return facetFormattedResults;
            }

            // Filter virtual field values
            foreach (var originalQuery in facetQueries)
            {
                if (originalQuery.FieldNames.Count() > 1)
                {
                    throw new NotSupportedException("Pivot faceting is not supported by Azure Search provider.");
                }

                var fieldName = originalQuery.FieldNames.Single();

                if (!processedFacets.ContainsKey(fieldName))
                {
                    continue;
                }

                var categoryValues = processedFacets[fieldName];

                if (originalQuery.MinimumResultCount > 0)
                {
                    categoryValues = categoryValues.Where(cv => cv.Value >= originalQuery.MinimumResultCount).ToList();
                }

                var values = categoryValues.Select(v => new FacetValue(v.Key, v.Value));

                var fieldMap = (ICloudFieldMap)((CloudIndexConfiguration)this.context.Index.Configuration).FieldMap;
                var fieldConfig = fieldMap.GetCloudFieldConfigurationByCloudFieldName(originalQuery.CategoryName);
                var categoryName = fieldConfig == null ? originalQuery.CategoryName : fieldConfig.FieldName;

                facetFormattedResults.Categories.Add(new FacetCategory(categoryName, values));
            }

            return facetFormattedResults;
        }

        private string OptimizeQueryExpression(CloudQuery query, CloudSearchProviderIndex index)
        {
            var expression = query.Expression;

            // If 'skip' is set then set 'start' to that value.
            var skipFields =
                query.Methods.Where(m => m.MethodType == QueryMethodType.Skip).Select(m => (SkipMethod)m).ToList();

            if (skipFields.Any())
            {
                var start = skipFields.Sum(skipMethod => skipMethod.Count);
                expression = expression + string.Format("&$skip={0}", start);
            }

            // If 'take' is set then return that number of rows.
            var takeFields =
                query.Methods.Where(m => m.MethodType == QueryMethodType.Take).Select(m => (TakeMethod)m).ToList();

            if (takeFields.Any())
            {
                var rows = takeFields.Sum(takeMethod => takeMethod.Count);
                expression = expression + string.Format("&$top={0}", rows);
            }

            var facetExpression = this.GetFacetExpression(query, index);

            expression = CloudQueryBuilder.Merge(expression, facetExpression, "and", CloudQueryBuilder.ShouldWrap.Both);

            var orderByExpression = this.GetOrderByExpression(query, index);
            return $"{expression}{orderByExpression}";
        }

        private string GetFacetExpression(CloudQuery query, CloudSearchProviderIndex index)
        {
            string expression = string.Empty;
            // If 'GetFacets' is found then add the facets into this request
            var getResultsFields =
                query.Methods.Where(m => m.MethodType == QueryMethodType.GetResults).Select(m => (GetResultsMethod)m).ToList();
            var facetFields =
                query.Methods.Where(m => m.MethodType == QueryMethodType.GetFacets).Select(m => (GetFacetsMethod)m).ToList();
            if (query.FacetQueries.Count > 0 && (facetFields.Any() || getResultsFields.Any()))
            {
                // Process virtual fields. Temporary no effect for cloud
                var result = GetFacetsPipeline.Run(
                    this.pipeline,
                    new GetFacetsArgs(
                        null,
                        query.FacetQueries,
                        this.context.Index.Configuration.VirtualFieldProcessors,
                        this.context.Index.FieldNameTranslator));
                var facetQueriesRaw = result.FacetQueries.ToHashSet();
                var facetQueries = new List<FacetQuery>();

                foreach (var facetQuery in facetQueriesRaw)
                {
                    if (facetQueries.Any(x => x.FieldNames.SequenceEqual(facetQuery.FieldNames)))
                    {
                        continue;
                    }

                    facetQueries.Add(facetQuery);
                }

                foreach (var facetQuery in facetQueries)
                {
                    if (!facetQuery.FieldNames.Any())
                    {
                        continue;
                    }

                    foreach (var fieldName in facetQuery.FieldNames)
                    {
                        var indexFieldName = this.FieldNameTranslator.GetIndexFieldName(fieldName);

                        var schema = (this.context.Index.Schema as ICloudSearchIndexSchema);
                        var fieldSchema = schema.GetFieldByCloudName(indexFieldName);
                        if (fieldSchema == null)
                        {
                            continue;
                        }

                        var facet = string.Format("&facet={0}", indexFieldName);

                        if (index.MaxTermsCountInFacet != 0)
                        {
                            facet += string.Format(",sort:count,count:{0}", index.MaxTermsCountInFacet);
                        }

                        if (facetQuery.FilterValues != null)
                        {
                            var facetExpression = string.Empty;

                            foreach (var filterValue in facetQuery.FilterValues)
                            {
                                if (filterValue is string)
                                {
                                    facetExpression = CloudQueryBuilder.Merge(
                                        facetExpression,
                                        CloudQueryBuilder.Search.Operations.Equal(indexFieldName, filterValue, 1),
                                        "or");
                                }
                                else
                                {
                                    facetExpression = CloudQueryBuilder.Merge(
                                        facetExpression,
                                        CloudQueryBuilder.Filter.Operations.Equal(indexFieldName, filterValue, fieldSchema.Type),
                                        "or");
                                }
                            }

                            expression = CloudQueryBuilder.Merge(
                                expression,
                                facetExpression,
                                "and",
                                CloudQueryBuilder.ShouldWrap.Right);
                        }

                        expression = CloudQueryBuilder.Merge(expression, facet, "and", CloudQueryBuilder.ShouldWrap.Right);
                    }
                }
            }
            return expression;
        }

        private string GetOrderByExpression(CloudQuery query, CloudSearchProviderIndex index)
        {
            var sortFields =
                query.Methods.Where(m => m.MethodType == QueryMethodType.OrderBy).Select(m => ((OrderByMethod)m)).ToList();
            if (!sortFields.Any())
            {
                return string.Empty;
            }

            var sortDistinctFields = sortFields.GroupBy(x => x.Field).Select(x => x.First());
            var orderStringBuilder = new StringBuilder();
            foreach (var sortField in sortDistinctFields)
            {
                var fieldName = sortField.Field;
                var indexFieldName = this.context.Index.FieldNameTranslator.GetIndexFieldName(fieldName, typeof(TItem));

                if (!index.SearchService.Schema.AllFieldNames.Contains(indexFieldName))
                {
                    continue;
                }

                orderStringBuilder.Append(orderStringBuilder.ToString().Contains("$orderby") ? "," : "&$orderby=");
                orderStringBuilder.Append(indexFieldName);
                orderStringBuilder.Append(sortField.SortDirection == SortDirection.Descending ? " desc" : string.Empty);
            }

            return orderStringBuilder.ToString();
        }
    }
}