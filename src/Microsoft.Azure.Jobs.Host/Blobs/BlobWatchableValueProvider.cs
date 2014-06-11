﻿using System;
using System.IO;
using Microsoft.Azure.Jobs.Host.Bindings;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.Azure.Jobs.Host.Blobs
{
    internal sealed class BlobWatchableValueProvider : IValueProvider, IWatchable
    {
        private readonly ICloudBlob _blob;
        private readonly object _value;
        private readonly Type _valueType;
        private readonly ISelfWatch _watcher;

        public BlobWatchableValueProvider(ICloudBlob blob, object value, Type valueType, ISelfWatch watcher)
        {
            if (value != null && !valueType.IsAssignableFrom(value.GetType()))
            {
                throw new InvalidOperationException("value is not of the correct type.");
            }

            _blob = blob;
            _value = value;
            _valueType = valueType;
            _watcher = watcher;
        }

        public Type Type
        {
            get { return typeof(Stream); }
        }

        public object GetValue()
        {
            return _value;
        }

        public string ToInvokeString()
        {
            return _blob.GetBlobPath();
        }

        public ISelfWatch Watcher
        {
            get { return _watcher; }
        }
    }
}