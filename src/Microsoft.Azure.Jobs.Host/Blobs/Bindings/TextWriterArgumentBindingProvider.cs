﻿using System;
using System.IO;
using System.Reflection;
using System.Text;
using Microsoft.Azure.Jobs.Host.Bindings;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.Azure.Jobs.Host.Blobs.Bindings
{
    internal class TextWriterArgumentBindingProvider : IBlobArgumentBindingProvider
    {
        public IBlobArgumentBinding TryCreate(ParameterInfo parameter, FileAccess? access)
        {
            if (parameter.ParameterType != typeof(TextWriter))
            {
                return null;
            }

            if (access.HasValue && access.Value != FileAccess.Write)
            {
                throw new InvalidOperationException("Cannot bind blob to TextWriter using access "
                    + access.Value.ToString() + ".");
            }

            return new TextWriterArgumentBinding();
        }

        private class TextWriterArgumentBinding : IBlobArgumentBinding
        {
            public FileAccess Access
            {
                get { return FileAccess.Write; }
            }

            public Type ValueType
            {
                get { return typeof(TextWriter); }
            }

            public IValueProvider Bind(ICloudBlob blob, ArgumentBindingContext context)
            {
                CloudBlockBlob blockBlob = blob as CloudBlockBlob;

                if (blockBlob == null)
                {
                    throw new InvalidOperationException("Cannot bind a page blob to a TextWriter.");
                }

                CloudBlobStream rawStream = blockBlob.OpenWrite();
                IBlobCommitedAction committedAction = new BlobCommittedAction(blob, context.FunctionInstanceId,
                    context.NotifyNewBlob);
                SelfWatchCloudBlobStream selfWatchStream = new SelfWatchCloudBlobStream(rawStream, committedAction);
                const int defaultBufferSize = 1024;
                TextWriter writer = new StreamWriter(selfWatchStream, Encoding.UTF8, defaultBufferSize,
                    leaveOpen: true);
                return new TextWriterValueBinder(blob, selfWatchStream, writer);
            }

            // There's no way to dispose a CloudBlobStream without committing.
            // This class intentionally does not implement IDisposable because there's nothing it can do in Dispose.
            private class TextWriterValueBinder : IValueBinder, IWatchable
            {
                private readonly ICloudBlob _blob;
                private readonly SelfWatchCloudBlobStream _stream;
                private readonly TextWriter _value;

                public TextWriterValueBinder(ICloudBlob blob, SelfWatchCloudBlobStream stream, TextWriter value)
                {
                    _blob = blob;
                    _stream = stream;
                    _value = value;
                }

                public Type Type
                {
                    get { return typeof(TextWriter); }
                }

                public ISelfWatch Watcher
                {
                    get { return _stream; }
                }

                public object GetValue()
                {
                    return _value;
                }

                public void SetValue(object value)
                {
                    // Not ByRef, so can ignore value argument.
                    _value.Flush();
                    _value.Dispose();

                    // Determine whether or not to upload the blob.
                    if (_stream.Complete())
                    {
                        _stream.Dispose(); // Can only dispose when committing; see note on class above.
                    }
                }

                public string ToInvokeString()
                {
                    return _blob.GetBlobPath();
                }
            }
        }
    }
}