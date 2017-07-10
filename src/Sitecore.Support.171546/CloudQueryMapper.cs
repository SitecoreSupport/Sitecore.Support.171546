using Sitecore.ContentSearch.Azure.Http;
using Sitecore.ContentSearch.Azure.Query;
using Sitecore.ContentSearch.Azure.Schema;
using Sitecore.ContentSearch.Linq.Common;
using Sitecore.ContentSearch.Linq.Helpers;
using Sitecore.ContentSearch.Linq.Nodes;

namespace Sitecore.Support.ContentSearch.Azure.Query
{
    public class CloudQueryMapper : Sitecore.ContentSearch.Azure.Query.CloudQueryMapper
    {
        public CloudQueryMapper(CloudIndexParameters parameters) : base(parameters)
        {
        }

        private ICloudSearchIndexSchema Schema
        {
            get
            {
                return this.Parameters.Schema as ICloudSearchIndexSchema;
            }
        }

        protected override string HandleEqual(EqualNode node, CloudQueryMapperState state)
        {
            var onlyConstants = node.LeftNode is ConstantNode && node.RightNode is ConstantNode;

            if (onlyConstants)
            {
                var comparison = ((ConstantNode)node.LeftNode).Value.Equals(((ConstantNode)node.RightNode).Value);

                var expression = comparison
                                     ? CloudQueryBuilder.Search.SearchForEverything
                                     : CloudQueryBuilder.Search.SearchForNothing;

                return CloudQueryBuilder.Search.Operations.Equal(null, expression, node.Boost);
            }

            var fieldNode = QueryHelper.GetFieldNode(node);
            var valueNode = QueryHelper.GetValueNode(node, fieldNode.FieldType);

            string query = null;
            if (this.ProcessAsVirtualField(fieldNode.FieldKey, valueNode.Value, node.Boost, ComparisonType.Equal, state, out query))
            {
                return query;
            }

            return this.HandleEqual(fieldNode.FieldKey, valueNode.Value, node.Boost);
        }

        private string HandleEqual(string initFieldName, object fieldValue, float boost)
        {
            var fieldName = this.Parameters.FieldNameTranslator.GetIndexFieldName(initFieldName, this.Parameters.IndexedFieldType);
            var fieldSchema = this.Schema.GetFieldByCloudName(fieldName);
            if (fieldSchema == null)
            {
                var expression = fieldValue == null ?
                    CloudQueryBuilder.Search.SearchForEverything :
                    CloudQueryBuilder.Search.SearchForNothing;

                return CloudQueryBuilder.Search.Operations.Equal(null, expression, boost);
            }

            var formattedValue = this.ValueFormatter.FormatValueForIndexStorage(fieldValue, fieldName);

            if (fieldSchema.Type == EdmTypes.StringCollection)
            {
                return CloudQueryBuilder.Filter.Operations.Collections.Any(fieldName, formattedValue, fieldSchema.Type);
            }

            if (formattedValue == null)
            {
                return $"&$filter={fieldName} eq null";
            }

            if (formattedValue is string)
            {
                if (formattedValue.ToString().Trim() == string.Empty)
                {
                    return CloudQueryBuilder.Filter.Operations.Equal(fieldName, formattedValue, fieldSchema.Type, boost);
                }

                return Operations.Equal(fieldName, formattedValue, boost);
            }

            return CloudQueryBuilder.Filter.Operations.Equal(fieldName, formattedValue, fieldSchema.Type, boost);
        }
    }
}