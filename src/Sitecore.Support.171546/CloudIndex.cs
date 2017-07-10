using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Sitecore.ContentSearch.Azure.Query;
using Sitecore.ContentSearch.Linq.Common;
using Sitecore.ContentSearch.Linq.Indexing;
using Sitecore.ContentSearch.Linq.Parsing;

namespace Sitecore.Support.ContentSearch.Azure.Query
{
    public class CloudIndex<TItem> : Index<TItem, CloudQuery>
    {
        /// <summary>The query optimizer.</summary>
        private readonly CloudQueryOptimizer queryOptimizer = new CloudQueryOptimizer();

        /// <summary>The query mapper</summary>
        private readonly QueryMapper<CloudQuery> queryMapper;

        /// <summary>The parameters</summary>
        private readonly CloudIndexParameters parameters;

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudIndex{TItem}"/> class.
        /// </summary>
        public CloudIndex(CloudIndexParameters parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException("parameters");
            }

            this.queryMapper = new CloudQueryMapper(parameters);
            this.parameters = parameters;
        }

        #endregion

        public override TResult Execute<TResult>(CloudQuery query)
        {
            return default(TResult);
        }

        public override IEnumerable<TElement> FindElements<TElement>(CloudQuery query)
        {
            return new List<TElement>();
        }

        protected override QueryMapper<CloudQuery> QueryMapper
        {
            get
            {
                return this.queryMapper;
            }
        }

        protected override IQueryOptimizer QueryOptimizer
        {
            get
            {
                return this.queryOptimizer;
            }
        }

        protected override FieldNameTranslator FieldNameTranslator
        {
            get
            {
                return this.parameters.FieldNameTranslator;
            }
        }

        protected override IIndexValueFormatter ValueFormatter
        {
            get
            {
                return this.parameters.ValueFormatter;
            }
        }

        /// <summary>The parameters</summary>
        /// <value>The parameters.</value>
        public CloudIndexParameters Parameters
        {
            get
            {
                return this.parameters;
            }
        }
    }
}