using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serilog.Context; // For LogContext
// Removed: using SifarPriceProcessor.Models; // No longer needed
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SifarPriceProcessor // Root namespace
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly DataProcessor _dataProcessor;
        private readonly AppSettings _settings;
        private readonly TimeSpan _loopDelay;

        public Worker(ILogger<Worker> logger, DataProcessor dataProcessor, IOptions<AppSettings> settings)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
            _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));

            // Validate and set loop delay from settings
            if (_settings.WorkerLoopDelaySeconds <= 0)
            {
                _logger.LogWarning("AppSettings:WorkerLoopDelaySeconds is invalid ({Value}). Defaulting to 60 seconds.", _settings.WorkerLoopDelaySeconds);
                _loopDelay = TimeSpan.FromSeconds(60);
            }
            else
            {
                _loopDelay = TimeSpan.FromSeconds(_settings.WorkerLoopDelaySeconds);
            }
            _logger.LogInformation("Worker initialized. Loop delay set to {Delay} seconds.", _loopDelay.TotalSeconds);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker starting execution loop at: {time}", DateTimeOffset.Now);

            // Add a small initial delay before the first run, e.g., 5 seconds
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);


            while (!stoppingToken.IsCancellationRequested)
            {
                // Use LogContext to add properties specific to this execution run if needed
                using (LogContext.PushProperty("WorkerRunId", Guid.NewGuid()))
                {
                    _logger.LogInformation("Worker running processing task at: {time}", DateTimeOffset.Now);

                    try
                    {
                        // Call the main processing logic in DataProcessor
                        await _dataProcessor.ProcessSifarFilesAsync(stoppingToken);

                        _logger.LogInformation("Worker finished processing task.");
                    }
                    catch (Exception ex)
                    {
                        // Catch exceptions that might escape DataProcessor to prevent the worker loop from crashing
                        _logger.LogCritical(ex, "Unhandled exception occurred during DataProcessor execution. The worker loop will continue, but check the error.");
                        // Depending on the error, might want specific handling or even stopping the service.
                    }
                } // LogContext is disposed here

                // Wait for the configured delay before the next run
                if (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Worker delaying for {DelaySeconds} seconds before next run.", _loopDelay.TotalSeconds);
                    try
                    {
                        await Task.Delay(_loopDelay, stoppingToken);
                    }
                    catch (OperationCanceledException)
                    {
                        // This is expected when stoppingToken is cancelled during the delay
                        _logger.LogWarning("Task delay cancelled, worker stopping.");
                        break; // Exit the loop cleanly
                    }
                }
            } // End while loop

            _logger.LogInformation("Worker execution loop stopping at: {time}", DateTimeOffset.Now);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Worker StopAsync called.");
            // Perform any cleanup here if needed, like cancelling ongoing operations in DataProcessor gracefully
            // The stoppingToken passed to ExecuteAsync should handle cancellation of waits and loops.
            await base.StopAsync(cancellationToken);
            _logger.LogInformation("Worker stopped.");
        }
    }
}