// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Protocols;

namespace Microsoft.Azure.WebJobs.Host.Bindings
{
    // BindingProvider to bind to an exact type. Useful for binding to a client object. 
    internal class ExactTypeBindingProvider<TAttribute, TUserType> : IBindingProvider
        where TAttribute : Attribute
    {
        private readonly INameResolver _nameResolver;
        private readonly IConverterManager _converterManager;
        private readonly Func<TAttribute, Task<TUserType>> _buildFromAttr;
        private readonly Func<TAttribute, ParameterInfo, INameResolver, ParameterDescriptor> _buildParameterDescriptor;
        private readonly Func<TAttribute, ParameterInfo, INameResolver, Task<TAttribute>> _postResolveHook;

        public ExactTypeBindingProvider(
            INameResolver nameResolver,
            Func<TAttribute, Task<TUserType>> buildFromAttr,
            IConverterManager converterManager = null,
            Func<TAttribute, ParameterInfo, INameResolver, ParameterDescriptor> buildParameterDescriptor = null,
            Func<TAttribute, ParameterInfo, INameResolver, Task<TAttribute>> postResolveHook = null)
        {
            this._postResolveHook = postResolveHook;
            this._nameResolver = nameResolver;
            this._converterManager = converterManager ?? ConverterManager.Identity;
            this._buildParameterDescriptor = buildParameterDescriptor;
            this._buildFromAttr = buildFromAttr;
        }

        public Task<IBinding> TryCreateAsync(BindingProviderContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException("context");
            }

            var parameter = context.Parameter;
            var typeFinal = parameter.ParameterType;

            if (typeFinal.IsByRef)
            {
                return Task.FromResult<IBinding>(null);
            }

            var type = typeof(ExactBinding<>).MakeGenericType(typeof(TAttribute), typeof(TUserType), typeFinal);
            var method = type.GetMethod("TryBuild", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
            var binding = (IBinding)method.Invoke(null, new object[] { this, context });

            return Task.FromResult<IBinding>(binding);
        }

        private class ExactBinding<TFinalType> : BindingBase<TAttribute>
        {
            private readonly Func<TAttribute, Task<TUserType>> _buildFromAttr;
            private readonly FuncConverter<TUserType, TAttribute, TFinalType> _converter;

            public ExactBinding(
                AttributeCloner<TAttribute> cloner,
                ParameterDescriptor param,
                Func<TAttribute, Task<TUserType>> buildFromAttr,
                FuncConverter<TUserType, TAttribute, TFinalType> converter)
                : base(cloner, param)
            {
                _buildFromAttr = buildFromAttr;
                _converter = converter;
            }

            public static ExactBinding<TFinalType> TryBuild(
                ExactTypeBindingProvider<TAttribute, TUserType> parent,
                BindingProviderContext context)
            {
                var parameter = context.Parameter;
                TAttribute attributeSource = parameter.GetCustomAttribute<TAttribute>(inherit: false);

                if (attributeSource == null)
                {
                    return null;
                }

                // Only apply this rule if we can convert to the user's type. 
                FuncConverter<TUserType, TAttribute, TFinalType> converter = parent._converterManager.GetConverter<TUserType, TFinalType, TAttribute>();
                if (converter == null)
                {
                    return null;
                }
                
                Func<TAttribute, Task<TAttribute>> hookWrapper = null;
                if (parent._postResolveHook != null)
                {
                    hookWrapper = (attrResolved) => parent._postResolveHook(attrResolved, parameter, parent._nameResolver);
                }

                var cloner = new AttributeCloner<TAttribute>(attributeSource, context.BindingDataContract, parent._nameResolver, hookWrapper);
                ParameterDescriptor param;
                if (parent._buildParameterDescriptor != null)
                {
                    param = parent._buildParameterDescriptor(attributeSource, parameter, parent._nameResolver);
                }
                else
                {
                    param = new ParameterDescriptor
                    {
                        Name = parameter.Name,
                        DisplayHints = new ParameterDisplayHints
                        {
                            Description = "output"
                        }
                    };
                }

                var binding = new ExactBinding<TFinalType>(cloner, param, parent._buildFromAttr, converter);
                return binding;
            }

            protected override async Task<IValueProvider> BuildAsync(TAttribute attrResolved, ValueBindingContext context)
            {
                string invokeString = Cloner.GetInvokeString(attrResolved);
                var obj = await _buildFromAttr(attrResolved);

                var obj2 = _converter(obj, attrResolved, context);

                IValueProvider vp = new ConstantValueProvider(obj2, typeof(TFinalType), invokeString);
                return vp;
            }
        }
    }
}