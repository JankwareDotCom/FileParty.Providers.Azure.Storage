using FileParty.Core.Models;

namespace FileParty.Providers.Azure.Storage
{
    public abstract class AzureBlobBaseConfiguration : StorageProviderConfiguration<AzureModule>
    {
        public string ConnectionString { get; set; }
        public string ContainerName { get; set; }
        public bool AllowModificationsDuringRead { get; set; } = true;
        public override char DirectorySeparationCharacter => '/';
    }
}