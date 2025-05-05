using Azure.Identity;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Graph;
using Serilog;
using System;
using System.Collections.Generic; // Required for Dictionary within AppSettings
using System.Threading.Tasks;

namespace SifarPriceProcessor // Root namespace
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateBootstrapLogger();

            try
            {
                Log.Information("Starting SifarPriceProcessor host builder...");
                var host = CreateHostBuilder(args).Build();
                Log.Information("SifarPriceProcessor host built. Running...");
                await host.RunAsync();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "SifarPriceProcessor host terminated unexpectedly.");
            }
            finally
            {
                Log.Information("SifarPriceProcessor host shutting down.");
                await Log.CloseAndFlushAsync();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .UseSerilog((context, services, configuration) => configuration
                    .ReadFrom.Configuration(context.Configuration)
                    .ReadFrom.Services(services)
                    .Enrich.FromLogContext()
                    .Enrich.WithMachineName()
                    .Enrich.WithThreadId())
                .ConfigureServices((hostContext, services) =>
                {
                    // --- Configuration Binding ---
                    services.Configure<AppSettings>(hostContext.Configuration.GetSection("AppSettings"));

                    // --- Register GraphServiceClient ---
                    services.AddSingleton(provider =>
                    {
                        var logger = provider.GetRequiredService<ILogger<Program>>();
                        var configuration = provider.GetRequiredService<IConfiguration>();

                        // Direct configuration access
                        var tenantId = configuration["AppSettings:AzureAd:TenantId"];
                        var clientId = configuration["AppSettings:AzureAd:ClientId"];
                        var clientSecret = configuration["AppSettings:AzureAd:ClientSecret"];

                        // Log what we found
                        logger.LogInformation("Azure AD settings loaded - TenantId: '{TenantId}', ClientId: '{ClientId}', ClientSecret length: {SecretLength}",
                            tenantId ?? "null",
                            clientId ?? "null",
                            clientSecret?.Length ?? 0);

                        if (string.IsNullOrWhiteSpace(tenantId) ||
                            string.IsNullOrWhiteSpace(clientId) ||
                            string.IsNullOrWhiteSpace(clientSecret))
                        {
                            logger.LogCritical("Azure AD settings (TenantId, ClientId, ClientSecret) are missing or invalid in configuration. Cannot initialize GraphServiceClient.");
                            throw new InvalidOperationException("Azure AD settings (TenantId, ClientId, ClientSecret) are missing or invalid in configuration.");
                        }

                        logger.LogInformation("Creating GraphServiceClient using ClientSecretCredential for TenantId: {TenantId}, ClientId: {ClientId}",
                                             tenantId, clientId);

                        var credentials = new ClientSecretCredential(
                            tenantId,
                            clientId,
                            clientSecret);

                        string[] scopes = new[] { "https://graph.microsoft.com/.default" };
                        var graphClient = new GraphServiceClient(credentials, scopes);

                        logger.LogInformation("GraphServiceClient created successfully.");
                        return graphClient;
                    });

                    // --- Register Custom Services ---
                    services.AddSingleton<DataProcessor>();

                    // --- Register Background Worker ---
                    services.AddHostedService<Worker>();

                });
    }

    // =========================================================================
    //                            CLASS DEFINITIONS
    // =========================================================================

    // Main class to hold all application settings sections
    public class AppSettings
    {
        public int WorkerLoopDelaySeconds { get; set; }
        public AzureAdSettings AzureAd { get; set; }
        public SharePointSettings SharePoint { get; set; }
        public SqlSettings Sql { get; set; }
        public ExcelMappingSettings ExcelMapping { get; set; }
    }

    // Section for Azure Active Directory configuration
    public class AzureAdSettings
    {
        public string TenantId { get; set; }
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
    }

    // Section for SharePoint configuration
    public class SharePointSettings
    {
        public string SiteName { get; set; }
        public string InputFolderName { get; set; }
        public string ProcessedFolderName { get; set; }
        // ErrorFolderName is defined but no longer used in DataProcessor logic
        public string ErrorFolderName { get; set; }
    }

    // Section for SQL Server configuration
    public class SqlSettings
    {
        public string ConnectionString { get; set; }
        public string TargetTable { get; set; }
    }

    // Section for Excel file parsing configuration
    public class ExcelMappingSettings
    {
        public string SheetName { get; set; }
        public int HeaderRowIndex { get; set; }
        public int DataStartRowIndex { get; set; }
        // Maps PriceRecord property names to Excel column header text
        public Dictionary<string, string> Columns { get; set; }
    }

    // --- CORRECTED PriceRecord Class Definition ---
    // Represents data read from ONE row of the SIFAR Excel file
    public class PriceRecord
    {
        // Properties matching the KEYS in appsettings.json ExcelMapping.Columns
        // and used in DataProcessor.cs
        public string Brand { get; set; }           // From Excel "Brand" column
        public string PartNumber { get; set; }      // From Excel "P/N" column
        public int? Quantity { get; set; }          // From Excel "Q.TY" column (nullable int)
        public decimal? OfferPrice { get; set; }    // From Excel "Offer" column (nullable decimal)
        public string Comment { get; set; }         // From Excel "Uwagi" column (example)
        public string Description { get; set; }     // From Excel "Description" column - ADDED

        // Metadata properties
        public string SourceFileName { get; set; }
        public DateTime? LoadDateTime { get; set; }
    }

} // End of SifarPriceProcessor namespace