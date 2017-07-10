using Sitecore.ContentSearch.Azure;
using Sitecore.ContentSearch.Azure.Query;

namespace Sitecore.Support.ContentSearch.Azure.Query
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Globalization;
    using System.Linq;

    using Newtonsoft.Json.Linq;

    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Azure.FieldMaps;
    using Sitecore.ContentSearch.Linq;
    using Sitecore.ContentSearch.Linq.Common;
    using Sitecore.ContentSearch.Linq.Methods;

    public struct CloudSearchResults<TElement>
    {
        /// <summary>
        /// The select method.
        /// </summary>
        private readonly SelectMethod selectMethod;

        /// <summary>
        /// The search hits.
        /// </summary>
        private readonly List<Dictionary<string, object>> searchResults;

        /// <summary>
        /// The context.
        /// </summary>
        private readonly IProviderSearchContext context;

        /// <summary>
        /// The cloud index configuration.
        /// </summary>
        private readonly CloudIndexConfiguration cloudIndexConfiguration;

        /// <summary>
        /// The mapper.
        /// </summary>
        private readonly IIndexDocumentPropertyMapper<Dictionary<string, object>> mapper;

        /// <summary>
        /// The total number of matches found in the index.
        /// </summary>
        private readonly int numberFound;

        /// <summary>
        /// The facet result from cloud.
        /// </summary>
        private readonly Dictionary<string, object> facetResult;

        private readonly IIndexParameters parameters;

        private readonly IEnumerable<IFieldQueryTranslator> virtualFieldProcessors;

        /// <summary>
        /// Initializes a new instance of the <see>
        ///         <cref>CloudSearchResults</cref>
        ///     </see>
        ///     class.
        /// </summary>
        public CloudSearchResults(
            IProviderSearchContext context,
            List<Dictionary<string, object>> searchResults,
            SelectMethod selectMethod,
            int totalDoc,
            Dictionary<string, object> facetResult,
            IIndexParameters parameters,
            IEnumerable<IFieldQueryTranslator> virtualFieldProcessors)
        {
            this.context = context;
            this.cloudIndexConfiguration = (CloudIndexConfiguration)this.context.Index.Configuration;
            this.mapper = this.cloudIndexConfiguration.IndexDocumentPropertyMapper;
            this.searchResults = searchResults;
            this.selectMethod = selectMethod;
            this.numberFound = totalDoc;
            this.facetResult = facetResult;
            this.parameters = parameters;
            this.virtualFieldProcessors = virtualFieldProcessors;
        }

        public CloudIndexParameters Parameters
        {
            get
            {
                return this.parameters as CloudIndexParameters;
            }
        }

        public long Count()
        {
            return this.numberFound;
        }

        public IEnumerable<SearchHit<TElement>> GetSearchHits()
        {
            foreach (var searchResult in this.searchResults)
            {
                float score = -1;

                object scoreObj;

                if (searchResult.TryGetValue("@search.score", out scoreObj))
                {
                    if (scoreObj is double)
                    {
                        score = float.Parse(scoreObj.ToString());
                    }
                }

                yield return new SearchHit<TElement>(score, this.Map(searchResult));
            }
        }

        public bool Any()
        {
            return this.numberFound > 0;
        }

        public Dictionary<string, ICollection<KeyValuePair<string, int>>> GetFacets()
        {
            var facetData = this.facetResult;
            var facetFinalResults = new Dictionary<string, ICollection<KeyValuePair<string, int>>>();
            if (facetData == null)
            {
                return null;
            }

            foreach (var key in facetData.Keys)
            {
                if (key.Contains("@odata.type"))
                {
                    continue;
                }

                // Convert the data to a proper json and convert
                var values =
                    JObject.Parse("{\"" + key + "\":" + facetData[key] + "}")
                        .GetValue(key)
                        .ToObject<ICollection<Dictionary<string, object>>>();
                var swapKeyValues = new Collection<KeyValuePair<string, int>>();
                var configs = (this.cloudIndexConfiguration.FieldMap as ICloudFieldMap).GetCloudFieldConfigurationByCloudFieldName(key);

                foreach (var facetItem in values)
                {
                    var count = int.Parse(facetItem["count"].ToString());
                    var value = facetItem["value"].ToString();

                    if (configs != null)
                    {
                        var originalValue = configs.FormatForReading(value);
                        var parsedValue = this.cloudIndexConfiguration.IndexFieldStorageValueFormatter.ReadFromIndexStorage(originalValue, key, configs.Type);
                        var arrayValue = parsedValue as Array;
                        if (configs.Type.IsArray && arrayValue != null && arrayValue.Length == 1)
                        {
                            parsedValue = arrayValue.GetValue(0);
                        }

                        if (parsedValue is IFormattable && !string.IsNullOrEmpty(configs.Format))
                        {
                            parsedValue = ((IFormattable)parsedValue).ToString(configs.Format, CultureInfo.InvariantCulture);
                        }

                        value = parsedValue.ToString();
                    }

                    swapKeyValues.Add(new KeyValuePair<string, int>(value, count));
                }

                facetFinalResults.Add(key, swapKeyValues);
            }

            return facetFinalResults;
        }

        public IEnumerable<TElement> GetSearchResults()
        {
            foreach (var searchResult in this.searchResults)
            {
                yield return this.Map(searchResult);
            }
        }

        public TElement ElementAt(int index)
        {
            if (index < 0 || index > this.searchResults.Count)
            {
                throw new IndexOutOfRangeException();
            }

            return this.Map(this.searchResults[index]);
        }

        public TElement ElementAtOrDefault(int index)
        {
            if (index < 0 || index > this.searchResults.Count)
            {
                return default(TElement);
            }

            return this.Map(this.searchResults[index]);
        }

        public TElement First()
        {
            if (this.Count() < 1)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            return this.ElementAt(0);
        }

        public TElement FirstOrDefault()
        {
            if (this.Count() < 1)
            {
                return default(TElement);
            }

            return this.ElementAt(0);
        }

        public TElement Last()
        {
            if (this.searchResults.Count < 1)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            return this.ElementAt(this.searchResults.Count - 1);
        }

        public TElement LastOrDefault()
        {
            if (this.searchResults.Count < 1)
            {
                return default(TElement);
            }

            return this.ElementAt(this.searchResults.Count - 1);
        }

        public TElement Single()
        {
            if (this.Count() < 1)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            if (this.Count() > 1)
            {
                throw new InvalidOperationException("Sequence contains more than one element");
            }

            return this.Map(this.searchResults[0]);
        }

        public TElement SingleOrDefault()
        {
            if (this.Count() == 0)
            {
                return default(TElement);
            }

            if (this.Count() == 1)
            {
                return this.Map(this.searchResults[0]);
            }

            throw new InvalidOperationException("Sequence contains more than one element");
        }

        private TElement Map(IDictionary<string, object> fields)
        {
            // Remove Azure's system fields that starts with '@' 
            var elementFields = fields.Where(x => !x.Key.StartsWith("@")).ToDictionary(k => k.Key, v => v.Value);

            return this.mapper.MapToType<TElement>(
                elementFields,
                this.selectMethod,
                this.virtualFieldProcessors,
                this.Parameters?.ExecutionContexts ?? new IExecutionContext[0],
                this.context.SecurityOptions);
        }
    }
}