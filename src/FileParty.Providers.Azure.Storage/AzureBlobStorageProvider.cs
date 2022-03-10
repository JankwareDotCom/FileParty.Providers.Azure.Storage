using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using FileParty.Core.Enums;
using FileParty.Core.EventArgs;
using FileParty.Core.Interfaces;
using FileParty.Core.Models;

namespace FileParty.Providers.Azure.Storage
{
    public class AzureBlobStorageProvider : IAsyncStorageProvider, IStorageProvider
    {
        public char DirectorySeparatorCharacter { get; }

        private readonly string _connectionString;
        private readonly string _containerName;
        private readonly bool _allowModifications;
        
        public AzureBlobStorageProvider(StorageProviderConfiguration<AzureModule> configuration)
        {
            if (configuration is AzureBlobBaseConfiguration config)
            {
                _connectionString = config.ConnectionString;
                _containerName = config.ContainerName;
                _allowModifications = config.AllowModificationsDuringRead;
            }
            DirectorySeparatorCharacter = configuration.DirectorySeparationCharacter;
        }

        BlobServiceClient GetClient()
        {
            return new BlobServiceClient(_connectionString);
        }

        BlobClient GetClient(string storagePointer)
        {
            return GetClient()
                .GetBlobContainerClient(_containerName)
                .GetBlobClient(storagePointer);
        }
        
        public async Task<Stream> ReadAsync(string storagePointer, CancellationToken cancellationToken = new CancellationToken())
        {
            var client = GetClient(storagePointer);

            if (await client.ExistsAsync(cancellationToken))
            {
                return await client.OpenReadAsync(_allowModifications, cancellationToken: cancellationToken);
            }

            throw Core.Exceptions.Errors.FileNotFoundException;
        }

        public async Task<bool> ExistsAsync(string storagePointer, CancellationToken cancellationToken = new CancellationToken())
        {
            var type = await TryGetStoredItemTypeAsync(storagePointer, cancellationToken);
            return type != null;
        }

        public Task<IDictionary<string, bool>> ExistsAsync(IEnumerable<string> storagePointers, CancellationToken cancellationToken = new CancellationToken())
        {
            return Task.FromResult((IDictionary<string, bool>) storagePointers
                .Distinct(StringComparer.InvariantCultureIgnoreCase)
                .ToDictionary(
                    k => k,
                    v => TryGetStoredItemTypeAsync(v, cancellationToken).Result != null));
        }

        public async Task<StoredItemType?> TryGetStoredItemTypeAsync(string storagePointer, CancellationToken cancellationToken = new CancellationToken())
        {
            var baseClient = GetClient().GetBlobContainerClient(_containerName);

            var blobs = baseClient.GetBlobsAsync(prefix: storagePointer.Trim());
            var enumerator = blobs.GetAsyncEnumerator(cancellationToken);
            StoredItemType? type = null;

            while (await enumerator.MoveNextAsync())
            {
                var blob = enumerator.Current;
                type = blob.Name.Trim().TrimEnd(DirectorySeparatorCharacter)
                    .Equals(storagePointer.Trim().TrimEnd(DirectorySeparatorCharacter), 
                        StringComparison.InvariantCultureIgnoreCase)
                    ? StoredItemType.File
                    : StoredItemType.Directory;
                break;
            }

            return type;
        }

        public async Task<IStoredItemInformation> GetInformationAsync(string storagePointer, CancellationToken cancellationToken = new CancellationToken())
        {
            var type = await TryGetStoredItemTypeAsync(storagePointer, cancellationToken);
            
            if (type == null)
            {
                throw Core.Exceptions.Errors.FileNotFoundException;
            }
            
            var info = GetClient(storagePointer);

            var props = type == StoredItemType.File
                ? await info.GetPropertiesAsync(new BlobRequestConditions(), cancellationToken)
                : null;
            
            var pathParts =
                storagePointer
                    .Split(DirectorySeparatorCharacter) 
                    .Where(w => !string.IsNullOrWhiteSpace(w))
                    .ToList();

            var name = pathParts.Last();
            pathParts.Remove(name);
            var dirPath = string.Join(DirectorySeparatorCharacter.ToString(), pathParts);
            
            var result = new StoredItemInformation
            {
                CreatedTimestamp = props?.Value.CreatedOn.UtcDateTime,
                DirectoryPath = dirPath,
                LastModifiedTimestamp = props?.Value.LastModified.UtcDateTime,
                Name = name,
                Size = props?.Value.ContentLength,
                StoragePointer = info.Name,
                StoredType = (StoredItemType) type
            };

            return result;
        }

        public async Task WriteAsync(FilePartyWriteRequest request, CancellationToken cancellationToken = default)
        {
            var client = GetClient(request.StoragePointer);
            var progressHandler = new Progress<long>();
            var totalFileBytes = request.Stream.Length;

            if (request.WriteMode != WriteMode.CreateOrReplace)
            {
                var exists = await client.ExistsAsync(cancellationToken);
                if (request.WriteMode == WriteMode.Create && exists)
                {
                    throw Core.Exceptions.Errors.FileAlreadyExistsException;
                }
                
                if (request.WriteMode == WriteMode.Replace && !exists)
                {
                    throw Core.Exceptions.Errors.FileNotFoundException;
                }
            }
            

            progressHandler.ProgressChanged += (_, totalBytesTransferred) =>
            {
                WriteProgressEvent?.Invoke(
                    this, 
                    new WriteProgressEventArgs(
                        request.Id,
                        request.StoragePointer, 
                        totalBytesTransferred, 
                        totalFileBytes));
            };

            await client.UploadAsync(
                request.Stream, 
                progressHandler: progressHandler,
                cancellationToken: cancellationToken);
        }

        public async Task WriteAsync(string storagePointer, Stream stream, WriteMode writeMode,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await WriteAsync(
                new FilePartyWriteRequest(
                    storagePointer,
                    stream,
                    writeMode), 
                cancellationToken);
        }

        public async Task DeleteAsync(string storagePointer, CancellationToken cancellationToken = new CancellationToken())
        {
            await DeleteAsync(new List<string> {storagePointer}, cancellationToken);
        }

        public async Task DeleteAsync(IEnumerable<string> storagePointers, CancellationToken cancellationToken = new CancellationToken())
        {
            var client = GetClient();
            var containerClient = client.GetBlobContainerClient(_containerName);
            
            foreach (var sp in storagePointers)
            {
                var blobs = containerClient
                    .GetBlobsAsync(prefix: sp.Trim().TrimEnd(DirectorySeparatorCharacter));

                var enumerator = blobs.GetAsyncEnumerator(cancellationToken);
                while (await enumerator.MoveNextAsync())
                {
                    var blob = enumerator.Current;
                    await GetClient(blob.Name)
                        .DeleteAsync(cancellationToken: cancellationToken);
                }
            }
        }

        public void Write(FilePartyWriteRequest request)
        {
            WriteAsync(request, CancellationToken.None).Wait();
        }

        public void Write(string storagePointer, Stream stream, WriteMode writeMode)
        {
            WriteAsync(storagePointer, stream, writeMode, CancellationToken.None).Wait();
        }

        public void Delete(string storagePointer)
        {
            DeleteAsync(storagePointer).Wait();
        }

        public void Delete(IEnumerable<string> storagePointers)
        {
            DeleteAsync(storagePointers, CancellationToken.None).Wait();
        }

        public event EventHandler<WriteProgressEventArgs> WriteProgressEvent;

        public Stream Read(string storagePointer)
        {
            return ReadAsync(storagePointer, CancellationToken.None).Result;
        }

        public bool Exists(string storagePointer)
        {
            return ExistsAsync(storagePointer, CancellationToken.None).Result;
        }

        public IDictionary<string, bool> Exists(IEnumerable<string> storagePointers)
        {
            return ExistsAsync(storagePointers, CancellationToken.None).Result;
        }

        public bool TryGetStoredItemType(string storagePointer, out StoredItemType? type)
        {
            type = TryGetStoredItemTypeAsync(storagePointer, CancellationToken.None).Result;
            return type != null;
        }

        public IStoredItemInformation GetInformation(string storagePointer)
        {
            return GetInformationAsync(storagePointer, CancellationToken.None).Result;
        }
    }
}
