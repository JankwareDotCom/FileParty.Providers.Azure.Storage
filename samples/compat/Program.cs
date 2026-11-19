using System;
using Azure.Storage.Blobs;
using FileParty.Core.Interfaces;
using FileParty.Core.Registration;
using FileParty.Providers.AWS.S3;
using FileParty.Providers.AWS.S3.Config;
using FileParty.Providers.Azure.Storage;
using FileParty.Providers.FileSystem;
using Microsoft.Extensions.DependencyInjection;

namespace FileParty.Compat
{
    // Touches the public registration surface of every package so the build fails if a type the
    // consumer depends on is missing or cannot bind against the pinned dependency versions.
    internal static class Program
    {
        private static int Main()
        {
            var azure = new AzureBlobConfiguration
            {
                ConnectionString = "UseDevelopmentStorage=true",
                ContainerName = "compat",
            };
            // BlobClientOptions comes from the consumer's own Azure.Storage.Blobs reference.
            azure.SetClientOptions(o => o.Retry.MaxRetries = 1);

            var services = new ServiceCollection()
                .AddFileParty(c => c
                    .AddModule(new FileSystemConfiguration(AppDomain.CurrentDomain.BaseDirectory))
                    .AddModule(new AWSDefaultConfiguration { Region = "us-east-1", Name = "compat-bucket" })
                    .AddModule<AzureModule>(azure)
                    .SetDefaultModule<FileSystemModule>());

            using (var provider = services.BuildServiceProvider())
            {
                var factory = provider.GetRequiredService<IFilePartyFactory>();
                Console.WriteLine($"FileParty compat OK: {factory.GetStorageProvider().GetType().FullName}");
                Console.WriteLine($"Azure provider: {typeof(AzureBlobStorageProvider).FullName}, client options set: {azure.ClientOptionsSet}, BlobClientOptions from {typeof(BlobClientOptions).Assembly.GetName().Version}");
            }

            return 0;
        }
    }
}
