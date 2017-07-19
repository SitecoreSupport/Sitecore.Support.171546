using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Sitecore.ContentSearch.Azure;
using Sitecore.ContentSearch.Azure.Http;

namespace Sitecore.Support.ContentSearch.Azure.Query
{
    public static class Operations
    {
        public static string Equal(string field, object expression, float boost)
        {
            if (!(expression is string))
            {
                throw new NotSupportedException("Only string expressions are supported by search");
            }

            if (string.IsNullOrEmpty(field) || field == "*")
            {
                return string.Format("&search={0}{1}", expression, boost != 1f ? "^" + boost : string.Empty);
            }

            return string.Format("&$filter={0} eq \'{1}\'", field, Escape((string)expression));
            //return string.Format("&search={0}:\"{1}\"{2}", field, Escape((string)expression), boost != 1f ? "^" + boost : string.Empty);
        }

        private static string Escape(string expression, bool leftWildcards = false)
        {
            var chars = @"\/+-&|!(){}[]^""'~*?:";

            if (expression.IndexOfAny(chars.ToArray()) != -1)
            {
                foreach (var ch in chars)
                {
                    if (leftWildcards && (ch == '*' || ch == '?'))
                    {
                        continue;
                    }

                    expression = expression.Replace(ch.ToString(), "\\" + ch);
                }
            }

            return expression;
        }

    }
}