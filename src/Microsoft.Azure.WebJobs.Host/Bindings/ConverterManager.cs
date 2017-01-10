// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs
{   
    // Concrete implementation of IConverterManager
    internal class ConverterManager : IConverterManager
    {
        // Map from <TSrc,TDest> to a converter function. 
        // (Type) --> FuncConverter<object, TAttribute, object>
        private readonly Dictionary<string, object> _funcsWithAttr = new Dictionary<string, object>();

        private readonly List<Entry> _openConverters = new List<Entry>();

        public static readonly IConverterManager Identity = new IdentityConverterManager();

        public ConverterManager()
        {
            this.AddConverter<byte[], string>(DefaultByteArray2String);
        }

        private void AddOpenConverter<TSrc, TDest, TAttribute>(
          Func<Type, Type, Func<object, object>> converterBuilder)
          where TAttribute : Attribute
        {
            var entry = new Entry
            {
                SourceCheck = GetChecker<TSrc>(),
                DestCheck = GetChecker<TDest>(),
                Attribute = typeof(TAttribute),
                Builder = converterBuilder
            };
            this._openConverters.Add(entry);
        }
         
        // Given a Type, get a predicate for determining if a given type matches it. 
        // This handles OpenTypes.        
        private static Func<Type, bool> GetChecker<TOpenType>()
        {
            var t = typeof(TOpenType);

            if (!typeof(OpenType).IsAssignableFrom(t))
            {
                // If not an OpenType, then must be an exact match. 
                return (type) => (type == t);
            }

            if (t == typeof(OpenType))
            {
                return (type) => true;
            }
            var method = t.GetMethod("IsValid", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            if (method == null)
            {
                throw new InvalidOperationException("Type " + t.FullName + "' is derived from " + typeof(OpenType).Name + " but is missing an " 
                    + "IsValid()" + " method");
            }

            var func = (Func<Type, bool>)method.CreateDelegate(typeof(Func<Type, bool>));
            return func;
        }

        private Func<Type, Type, Func<object, object>> TryGetOpenConverter(Type typeSource, Type typeDest, Type typeAttribute)
        {
            foreach (var entry in _openConverters)
            {
                if (entry.Attribute == typeAttribute)
                {
                    if (entry.SourceCheck(typeSource) && entry.DestCheck(typeDest))
                    {
                        return entry.Builder;
                    }
                }
            }
            return null;
        }

        private static string DefaultByteArray2String(byte[] bytes)
        {
            string str = Encoding.UTF8.GetString(bytes);
            return str;
        }

        private static string GetKey<TSrc, TDest, TAttribute>()
        {
            return typeof(TSrc).FullName + "|" + typeof(TDest).FullName + "|" + typeof(TAttribute).FullName;
        }

        public void AddConverterBuilder<TSrc, TDest, TAttribute>(
            Func<Type, Type, Func<object, object>> converterBuilder)
            where TAttribute : Attribute
        {
            if (typeof(OpenType).IsAssignableFrom(typeof(TSrc)) ||
                typeof(OpenType).IsAssignableFrom(typeof(TDest)))
            {
                AddOpenConverter<TSrc, TDest, TAttribute>(converterBuilder);
                return;
            }

            string key = GetKey<TSrc, TDest, TAttribute>();
            _funcsWithAttr[key] = converterBuilder;
        }

        public void AddConverter<TSrc, TDest, TAttribute>(FuncConverter<TSrc, TAttribute, TDest> converter)
            where TAttribute : Attribute
        {
            string key = GetKey<TSrc, TDest, TAttribute>();
            _funcsWithAttr[key] = converter;                        
        }

        // Return null if not found. 
        private FuncConverter<TSrc, TAttribute, TDest> TryGetConverter<TSrc, TAttribute, TDest>(
            Type typeSrc = null, Type typeDest = null)
            where TAttribute : Attribute
        {
            object obj;

            // First lookup specificially for TAttribute. 
            string key1 = GetKey<TSrc, TDest, TAttribute>();
            if (_funcsWithAttr.TryGetValue(key1, out obj))
            {
                var builder = obj as Func<Type, Type, Func<object, object>>;
                if (builder != null)
                {
                    Func<object, object> converter = builder(typeSrc, typeDest); // converter 
                    FuncConverter<TSrc, TAttribute, TDest> func3 = (input, attribute, context) =>
                    {
                        var output = converter(input);
                        return (TDest)output;
                    };
                    return func3;
                }

                var func = (FuncConverter<TSrc, TAttribute, TDest>)obj;
                return func;
            }

            // No specific case, lookup in the general purpose case. 
            string key2 = GetKey<TSrc, TDest, Attribute>();
            if (_funcsWithAttr.TryGetValue(key2, out obj))
            {
                var builder = obj as Func<Type, Type, Func<object, object>>;
                if (builder != null)
                {
                    Func<object, object> converter = builder(typeSrc, typeDest); // converter 
                    FuncConverter<TSrc, TAttribute, TDest> func3 = (input, attribute, context) =>
                    {
                        var output = converter(input);
                        return (TDest)output;
                    };
                    return func3;
                }

                var func1 = (FuncConverter<TSrc, Attribute, TDest>)obj;
                FuncConverter<TSrc, TAttribute, TDest> func2 = (src, attr, context) => func1(src, null, context);
                return func2;
            }

            return null;
        }

        public FuncConverter<TSrc, TAttribute, TDest> GetConverter<TSrc, TDest, TAttribute>()
            where TAttribute : Attribute
        {
            // Give precedence to exact matches.
            // this lets callers override any other rules (like JSON binding) 

            // TSrc --> TDest
            var exactMatch = TryGetConverter<TSrc, TAttribute, TDest>();
            if (exactMatch != null)
            {
                return exactMatch;
            }

            var typeSource = typeof(TSrc);
            var typeDest = typeof(TDest);

            {
                var builder = TryGetOpenConverter(typeof(TSrc), typeof(TDest), typeof(TAttribute));
                if (builder != null)
                {
                    var converter = builder(typeSource, typeDest);
                    return (src, attr, context) => (TDest)converter(src);
                }
            }

            // Object --> TDest
            // Catch all for any conversion to TDest
            var objConversion = TryGetConverter<object, TAttribute, TDest>(typeSource, typeDest);
            if (objConversion != null)
            {
                return (src, attr, context) =>
                {
                    var result = objConversion(src, attr, context);
                    return result;
                };
            }

            // Inheritence (also covers idempotency)
            if (typeof(TDest).IsAssignableFrom(typeof(TSrc)))
            {
                return (src, attr, context) =>
                {
                    object obj = (object)src;
                    return (TDest)obj;
                };
            }

            // string --> TDest
            var fromString = TryGetConverter<string, TAttribute, TDest>();
            if (fromString == null)
            {
                return null;
            }

            // String --> TDest
            if (typeof(TSrc) == typeof(string))
            {
                return (src, attr, context) =>
                {
                    var result = fromString((string)(object)src, attr, context);
                    return result;
                };
            }

            // Allow some well-defined intermediate conversions. 
            // If this is "wrong" for your type, then it should provide an exact match to override.

            // Byte[] --[builtin]--> String --> TDest
            if (typeof(TSrc) == typeof(byte[]))
            {
                var bytes2string = TryGetConverter<byte[], TAttribute, string>();

                return (src, attr, context) =>
                {
                    byte[] bytes = (byte[])(object)src;
                    string str = bytes2string(bytes, attr, context);
                    var result = fromString(str, attr, context);
                    return result;
                };
            }

            // General JSON serialization rule. 

            if (typeof(TSrc).IsPrimitive ||
               (typeof(TSrc) == typeof(object)) ||
                typeof(IEnumerable).IsAssignableFrom(typeof(TSrc)))
            {
                return null;
            }

            var funcJobj = TryGetConverter<object, TAttribute, JObject>();
            if (funcJobj == null)
            {
                funcJobj = (object obj, TAttribute attr, ValueBindingContext context) => JObject.FromObject(obj);
            }

            // TSrc --[Json]--> string --> TDest
            return (src, attr, context) =>
            {
                JObject jobj = funcJobj((object)src, attr, context);
                string json = jobj.ToString();
                TDest obj = fromString(json, attr, context);
                return obj;
            };
        }

        // List of open converters. Since these are not exact type matches, need to search through and determine the match. 
        private class Entry
        {
            public Func<Type, bool> SourceCheck { get; set; }
            public Func<Type, bool> DestCheck { get; set; }
            public Type Attribute { get; set; }

            public Func<Type, Type, Func<object, object>> Builder { get; set; }
        }

        // "Empty" converter manager that only allows identity conversions. 
        // This is useful for constrained rules that don't want to operate against exact types and skip 
        // arbitrary user conversions. 
        private class IdentityConverterManager : IConverterManager
        {
            public void AddConverter<TSource, TDestination, TAttribute1>(FuncConverter<TSource, TAttribute1, TDestination> converter) where TAttribute1 : Attribute
            {
                throw new NotImplementedException();
            }

            public void AddConverterBuilder<TSrc, TDest, TAttribute1>(Func<Type, Type, Func<object, object>> converterBuilder) where TAttribute1 : Attribute
            {
                throw new NotImplementedException();
            }

            public FuncConverter<TSource, TAttribute1, TDestination> GetConverter<TSource, TDestination, TAttribute1>() where TAttribute1 : Attribute
            {
                if (typeof(TSource) != typeof(TDestination))
                {
                    return null;
                }
                return (src, attr, ctx) =>
                {
                    object obj = (object)src;
                    return (TDestination)obj;
                };
            }
        }
    } // end class ConverterManager
}