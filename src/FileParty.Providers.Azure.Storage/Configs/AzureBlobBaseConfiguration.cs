using System;
using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using FileParty.Core.Models;

namespace FileParty.Providers.Azure.Storage
{
    public abstract class AzureBlobBaseConfiguration : StorageProviderConfiguration<AzureModule>
    {
        [NonSerialized]
        private Action<BlobClientOptions> _clientOptions = null;
        public string ConnectionString { get; set; }
        public string ContainerName { get; set; }
        public bool AllowModificationsDuringRead { get; set; } = true;
        public override char DirectorySeparationCharacter => '/';
        public bool ClientOptionsSet => _clientOptions != null;
        public AzureBlobBaseConfiguration SetClientOptions(Action<BlobClientOptions> options)
        {
            _clientOptions = options;
            return this;
        }

        public void ApplyClientOptions(BlobClientOptions options)
        {
            _clientOptions?.Invoke(options);
        }
    }
}