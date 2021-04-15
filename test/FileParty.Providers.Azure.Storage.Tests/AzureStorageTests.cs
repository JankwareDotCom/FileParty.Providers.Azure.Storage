using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FileParty.Core.Enums;
using FileParty.Core.Exceptions;
using FileParty.Core.Interfaces;
using FileParty.Core.Models;
using FileParty.Core.Registration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace FileParty.Providers.Azure.Storage.Tests
{
    public class AzureStorageProviderShould
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public AzureStorageProviderShould(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }
        
        [Fact]
        public async Task DoAllTheThingsOnce()
        {
            var sc = this.AddFileParty(
                c => c.AddModule<AzureModule>(new AzureBlobConfiguration
                {
                    ConnectionString = Environment.GetEnvironmentVariable("CONNECTION_STRING"),
                    ContainerName = "tests",
                    AllowModificationsDuringRead = true
                }));
            
            await using var sp = sc.BuildServiceProvider();
            var fileProvider = sp.GetRequiredService<IAsyncStorageProvider>();
            var sub = sp.GetRequiredService<IWriteProgressSubscriptionManager>();
            sub.SubscribeToAll((a, b) => _testOutputHelper.WriteLine($"{b.StoragePointer} - {b.PercentComplete}"));

            // cheat and clear all contents
            await fileProvider.DeleteAsync("myfile", CancellationToken.None);
            
            // check if file exists, it doesn't
            var storagePointer = $"myfile{fileProvider.DirectorySeparatorCharacter}thing.txt";
            Assert.False(await fileProvider.ExistsAsync(storagePointer));
            
            // create file
            await using var inputStream = new MemoryStream();
            await using var inputWriter = new StreamWriter(inputStream);
            await inputWriter.WriteAsync(new string('*', 12 * 1024)); // 12kb string
            await inputWriter.FlushAsync();
            inputStream.Position = 0;

            var request = new FilePartyWriteRequest(storagePointer, inputStream, WriteMode.Create);
            await fileProvider.WriteAsync(request, CancellationToken.None);

            // check if file exists, it does
            Assert.True(await fileProvider.ExistsAsync(storagePointer));
            
            // check if directory exists, it does
            Assert.True(await fileProvider.ExistsAsync("myfile"));

            // get file info
            var info = await fileProvider.GetInformationAsync(storagePointer, CancellationToken.None);
            Assert.NotNull(info);
            Assert.Equal(StoredItemType.File, info.StoredType);

            // get dir info
            var info2 = await fileProvider.GetInformationAsync("myfile", CancellationToken.None);
            Assert.NotNull(info2);
            Assert.Equal(StoredItemType.Directory, info2.StoredType);
            
            // try to overwrite file, but fail
            await using var inputStream2 = new MemoryStream();
            await using var inputWriter2 = new StreamWriter(inputStream2);
            await inputWriter2.WriteAsync(new string('/', 12 * 1024)); // 12kb string
            await inputWriter2.FlushAsync();
            inputStream2.Position = 0;
            request.Stream = inputStream2;
            var error = await Assert.ThrowsAsync<StorageException>(async () =>
            {
                await fileProvider.WriteAsync(request, CancellationToken.None);
            });
            
            Assert.Equal(Errors.FileAlreadyExistsException.Message, error.Message);

            // try to overwrite file, but succeed
            await using var inputStream3 = new MemoryStream();
            await using var inputWriter3 = new StreamWriter(inputStream3);
            await inputWriter3.WriteAsync(new string('-', 12 * 1024)); // 12kb string
            await inputWriter3.FlushAsync();
            inputStream3.Position = 0;
            request.Stream = inputStream3;
            request.WriteMode = WriteMode.CreateOrReplace;

            await fileProvider.WriteAsync(request, CancellationToken.None);
            await using var contents = await fileProvider.ReadAsync(storagePointer, CancellationToken.None);
            using var reader = new StreamReader(contents);
            var fileString = await reader.ReadToEndAsync();
            Assert.True(fileString.All(a=>a == '-'));

            // try to delete file
            await fileProvider.DeleteAsync(storagePointer, CancellationToken.None);
            Assert.False(await fileProvider.ExistsAsync(storagePointer, CancellationToken.None));
        }
    }
}