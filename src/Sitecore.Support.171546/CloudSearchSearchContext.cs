using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Search;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Linq.Common;
using Sitecore.ContentSearch.Security;
using Sitecore.ContentSearch.Utilities;
using Sitecore.Diagnostics;
using Sitecore.Support.ContentSearch.Azure.Query;

namespace Sitecore.Support.ContentSearch.Azure
{
    public class CloudSearchSearchContext : IProviderSearchContext
    {
        private readonly CloudSearchProviderIndex index;
        private readonly SearchSecurityOptions securityOptions;
        private IndexSearcher searcher;
        private bool disposed;
        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudSearchSearchContext"/> class.
        /// </summary>
        public CloudSearchSearchContext(CloudSearchProviderIndex index, SearchSecurityOptions options = SearchSecurityOptions.EnableSecurityCheck)
        {
            Assert.ArgumentNotNull(index, "index");
            Assert.ArgumentNotNull(options, "options");
            this.index = index;
            this.securityOptions = options;
        }

        #endregion

        public void Dispose()
        {
            if (this.searcher != null)
            {
                this.searcher.Dispose();
                this.searcher = null;
            }

            this.disposed = true;

            GC.SuppressFinalize(this);
        }

        public Searcher Searcher
        {
            get
            {
                if (this.disposed)
                {
                    throw new ObjectDisposedException(base.GetType().Name);
                }
                if (this.searcher == null)
                {
                    throw new NullReferenceException("Context not initialized.");
                }
                return this.searcher;
            }
        }

        public IQueryable<TItem> GetQueryable<TItem>()
        {
            return this.GetQueryable<TItem>(new IExecutionContext[0]);
        }

        public IQueryable<TItem> GetQueryable<TItem>(IExecutionContext executionContext)
        {
            return this.GetQueryable<TItem>(new[] { executionContext });
        }

        public IQueryable<TItem> GetQueryable<TItem>(params IExecutionContext[] executionContexts)
        {
            var indexItem = new LinqToCloudIndex<TItem>(this, executionContexts);
            if (this.index.Locator.GetInstance<IContentSearchConfigurationSettings>().EnableSearchDebug())
            {
                (indexItem as IHasTraceWriter).TraceWriter = new LoggingTraceWriter(SearchLog.Log);
            }
            return indexItem.GetQueryable();
        }

        public IEnumerable<SearchIndexTerm> GetTermsByFieldName(string fieldName, string prefix)
        {
            yield break;
        }

        public ISearchIndex Index
        {
            get
            {
                return this.index;
            }
        }

        public bool ConvertQueryDatesToUtc { get; set; }

        public SearchSecurityOptions SecurityOptions
        {
            get
            {
                return this.securityOptions;
            }
        }
    }
}