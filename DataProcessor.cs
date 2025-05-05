using ClosedXML.Excel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
// Using SDK v5 based on CCS Sample code
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Microsoft.Graph.Models.ODataErrors;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net; // Added for HttpStatusCode check
using System.Threading;
using System.Threading.Tasks;

namespace SparesPriceProcessor // Changed namespace
{
    public class DataProcessor
    {
        private readonly ILogger<DataProcessor> _logger;
        private readonly GraphServiceClient _graphClient;
        private readonly AppSettings _settings; // Uses AppSettings defined in Program.cs
        private readonly CultureInfo _cultureInfo = new CultureInfo("pl-PL"); // Or CultureInfo.InvariantCulture

        // Constructor remains the same
        public DataProcessor(
            ILogger<DataProcessor> logger,
            GraphServiceClient graphServiceClient,
            IOptions<AppSettings> appSettings)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _graphClient = graphServiceClient ?? throw new ArgumentNullException(nameof(graphServiceClient));
            _settings = appSettings?.Value ?? throw new ArgumentNullException(nameof(appSettings));

            // Validate required settings
            if (string.IsNullOrWhiteSpace(_settings.SharePoint?.SiteName) ||
                string.IsNullOrWhiteSpace(_settings.SharePoint?.InputFolderName) ||
                string.IsNullOrWhiteSpace(_settings.SharePoint?.ProcessedFolderName) || // Added check
                                                                                        // Removed ErrorFolderName check as it's no longer used
                                                                                        // string.IsNullOrWhiteSpace(_settings.SharePoint?.ErrorFolderName) ||
                string.IsNullOrWhiteSpace(_settings.Sql?.ConnectionString) ||
                string.IsNullOrWhiteSpace(_settings.Sql?.TargetTable) || // Added check
                _settings.ExcelMapping == null || // Added check
                _settings.ExcelMapping.HeaderRowIndex <= 0 || // Added check
                _settings.ExcelMapping.DataStartRowIndex <= 0 || // Added check
                _settings.ExcelMapping.Columns == null || // Added check
                !_settings.ExcelMapping.Columns.Any()) // Added check
            {
                throw new InvalidOperationException(
                    "One or more required AppSettings (SharePoint SiteName/Folders, SQL ConnectionString/TargetTable, ExcelMapping details) are missing or invalid.");
            }

            _logger.LogInformation("DataProcessor initialized. Input Folder: '{InputFolder}'", _settings.SharePoint.InputFolderName);
        }

        // Renamed main processing method
        // Modified main processing method - NO Error folder handling
        public async Task ProcessSifarFilesAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Starting SIFAR pricing file processing cycle.");

            string siteId = null;
            string driveId = null;
            DriveItem inputFolderItem = null;
            DriveItem processedFolderItem = null;
            // Removed: DriveItem errorFolderItem = null;
            List<DriveItem> excelFiles = new List<DriveItem>();

            try
            {
                // --- 1. Get Site and Drive IDs ---
                _logger.LogDebug("Getting site ID for Hostname: {SiteName}", _settings.SharePoint.SiteName);
                siteId = await GetSiteIdAsync(_settings.SharePoint.SiteName, cancellationToken);
                if (string.IsNullOrEmpty(siteId))
                {
                    _logger.LogError("Site '{SiteName}' not found. Aborting cycle.", _settings.SharePoint.SiteName);
                    return;
                }
                _logger.LogDebug("Site ID found: {SiteId}", siteId);

                _logger.LogDebug("Getting default drive ID for site {SiteId}", siteId);
                var drive = await _graphClient.Sites[siteId].Drive
                                .GetAsync(req => req.QueryParameters.Select = new[] { "id" }, cancellationToken);
                if (drive == null || string.IsNullOrEmpty(drive.Id))
                {
                    _logger.LogError("Could not retrieve default drive for site {SiteId}. Aborting cycle.", siteId);
                    return;
                }
                driveId = drive.Id;
                _logger.LogDebug("Default Drive ID found: {DriveId}", driveId);

                // --- 2. Get Folder DriveItems (Input & Processed only) ---
                _logger.LogDebug("Getting Input folder item: {InputFolder}", _settings.SharePoint.InputFolderName);
                inputFolderItem = await GetFolderItemByPathAsync(driveId, _settings.SharePoint.InputFolderName, cancellationToken);

                _logger.LogDebug("Getting Processed folder item: {ProcessedFolder}", _settings.SharePoint.ProcessedFolderName);
                processedFolderItem = await GetFolderItemByPathAsync(driveId, _settings.SharePoint.ProcessedFolderName, cancellationToken, true); // Allow creation

                // Removed: Error folder retrieval

                if (inputFolderItem == null || processedFolderItem == null) // Removed errorFolderItem check
                {
                    _logger.LogError("Could not retrieve or create required folders (Input, Processed). Aborting cycle.");
                    return;
                }

                // --- 3. List Excel Files ---
                _logger.LogDebug("Listing children in Input folder ID: {InputFolderId}", inputFolderItem.Id);
                var folderChildren = await _graphClient.Drives[driveId].Items[inputFolderItem.Id].Children
                                        .GetAsync(requestConfiguration =>
                                        {
                                            requestConfiguration.QueryParameters.Select = new[] { "id", "name", "file", "parentReference", "lastModifiedDateTime", "size" };
                                        }, cancellationToken);

                if (folderChildren?.Value == null)
                {
                    _logger.LogWarning("No items found in the input folder '{InputFolder}'.", _settings.SharePoint.InputFolderName);
                    return;
                }

                excelFiles = folderChildren.Value.Where(item =>
                    item.File != null &&
                    !item.Name.StartsWith("~$") &&
                    (item.Name.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase) ||
                     item.Name.EndsWith(".xls", StringComparison.OrdinalIgnoreCase))
                ).ToList();

                if (!excelFiles.Any())
                {
                    _logger.LogInformation("No Excel files found in the input folder '{InputFolder}'.", _settings.SharePoint.InputFolderName);
                    return;
                }
                _logger.LogInformation("Found {FileCount} Excel file(s) to process.", excelFiles.Count);

            }
            catch (ODataError e) when (e.ResponseStatusCode == (int)HttpStatusCode.NotFound)
            {
                _logger.LogError(e, "A required folder path was not found. Check SharePoint folder paths in configuration. Input='{Input}', Processed='{Processed}'", // Removed Error path from log
                    _settings.SharePoint.InputFolderName, _settings.SharePoint.ProcessedFolderName);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Critical error during SharePoint setup or file listing phase.");
                return;
            }


            // --- 4. Process Each File ---
            var allRecords = new List<PriceRecord>();
            var filesSuccessfullyParsed = new List<DriveItem>();

            foreach (var fileItem in excelFiles)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning("Cancellation requested during file processing loop.");
                    break;
                }

                _logger.LogInformation("Processing file: {FileName}...", fileItem.Name);
                bool success = false;
                Stream memoryStream = null;

                try
                {
                    // Download
                    _logger.LogDebug("Downloading file {FileName} (ID: {FileId})", fileItem.Name, fileItem.Id);
                    using (var stream = await _graphClient.Drives[driveId].Items[fileItem.Id].Content.GetAsync(null, cancellationToken))
                    {
                        if (stream == null) throw new InvalidOperationException($"Downloaded stream for {fileItem.Name} was null.");

                        // Create a memory stream without checking the original stream length
                        memoryStream = new MemoryStream();
                        await stream.CopyToAsync(memoryStream, cancellationToken);

                        // After copying, we can check if the memory stream has content
                        if (memoryStream.Length == 0)
                        {
                            throw new InvalidOperationException($"Downloaded content for {fileItem.Name} was empty (0 bytes).");
                        }

                        // Reset position for reading
                        memoryStream.Position = 0;
                        _logger.LogDebug("Successfully downloaded {FileName} ({Size} bytes)", fileItem.Name, memoryStream.Length);
                    }

                    // Parse
                    _logger.LogDebug("Parsing Excel file {FileName}", fileItem.Name);
                    var records = ParseExcelData(memoryStream, fileItem.Name); // Changed method name

                    if (records == null) throw new InvalidOperationException($"Critical error parsing Excel data from {fileItem.Name}.");
                    else if (records.Any())
                    {
                        allRecords.AddRange(records);
                        filesSuccessfullyParsed.Add(fileItem);
                        success = true;
                        _logger.LogInformation("Parsed {Count} records from {FileName}", records.Count, fileItem.Name);
                    }
                    else
                    {
                        _logger.LogWarning("No valid records parsed from {FileName}. File will remain in input folder.", fileItem.Name); // Updated log
                        success = false;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing file {FileName}. File will remain in input folder.", fileItem.Name); // Updated log
                    success = false;
                }
                finally
                {
                    memoryStream?.Dispose();
                }

                // --- File Move Logic ---
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning("Cancellation requested after processing {FileName}, skipping file move.", fileItem.Name);
                    continue;
                }

                if (success)
                {
                    // Mark for moving later after successful DB update
                    _logger.LogDebug("File {FileName} processed successfully, marked for potential move to Processed folder.", fileItem.Name);
                }
                else
                {
                    // REMOVED: No longer move to Error folder
                    _logger.LogWarning("File {FileName} failed processing and will remain in the Input folder.", fileItem.Name);
                }

            } // End foreach loop

            // --- 5. Database Update ---
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning("Cancellation requested before database update. Skipping DB operations.");
                return;
            }

            if (allRecords.Any())
            {
                _logger.LogInformation("Attempting database update with {TotalRecordCount} records from {ParsedFileCount} files.", allRecords.Count, filesSuccessfullyParsed.Count);
                bool dbSuccess = false;
                try
                {
                    await InsertDataAsync(allRecords, "Sifar Batch Process", cancellationToken);
                    dbSuccess = true;
                    _logger.LogInformation("Database update successful.");
                }
                catch (Exception dbEx)
                {
                    _logger.LogCritical(dbEx, "DATABASE UPDATE FAILED. Successfully parsed files will NOT be moved to Processed folder.");
                    dbSuccess = false;
                }

                // --- 6. Move Successfully Parsed Files AFTER DB Success ---
                if (dbSuccess)
                {
                    _logger.LogInformation("Moving {ParsedFileCount} successfully parsed files to Processed folder '{ProcessedFolder}'...", filesSuccessfullyParsed.Count, _settings.SharePoint.ProcessedFolderName);
                    int movedCount = 0;
                    foreach (var fileToMove in filesSuccessfullyParsed)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            _logger.LogWarning("Cancellation requested during final file move loop.");
                            break;
                        }
                        // Pass the already retrieved processedFolderItem.Id
                        bool moved = await MoveFileAsync(driveId, fileToMove, processedFolderItem.Id, cancellationToken);
                        if (moved) movedCount++;
                    }
                    _logger.LogInformation("Finished moving processed files. Moved {MovedCount}/{TotalCount} files.", movedCount, filesSuccessfullyParsed.Count);
                }
            }
            else
            {
                _logger.LogInformation("No records accumulated for database update.");
            }

            _logger.LogInformation("SIFAR pricing file processing cycle finished.");
        }

        // --- Helper Methods ---

        // --- Updated Excel Parsing Method ---
        // --- Updated Excel Parsing Method with clear mapping logic ---
        private List<PriceRecord> ParseExcelData(Stream excelStream, string sourceFileName)
        {
            _logger.LogDebug("Starting Excel parsing for file: {SourceFileName}", sourceFileName);
            var records = new List<PriceRecord>();
            var mapping = _settings.ExcelMapping;
            var columnConfig = mapping.Columns ?? new Dictionary<string, string>(); // Ensure Columns is not null

            try
            {
                using (var wb = new XLWorkbook(excelStream))
                {
                    _logger.LogDebug("Successfully opened Excel workbook {FileName}", sourceFileName);

                    IXLWorksheet ws = null;
                    // --- Sheet Selection Logic ---
                    if (!string.IsNullOrWhiteSpace(mapping.SheetName))
                    {
                        // Try to get the sheet by the configured name first
                        if (wb.Worksheets.TryGetWorksheet(mapping.SheetName, out ws))
                        {
                            _logger.LogDebug("Using worksheet specified in config: '{SheetName}'", mapping.SheetName);
                        }
                        else
                        {
                            // Configured sheet not found, try the first sheet as fallback
                            ws = wb.Worksheets.FirstOrDefault();
                            if (ws != null)
                            {
                                _logger.LogWarning("Worksheet '{ConfigSheetName}' not found in {FileName}. Falling back to the first sheet: '{ActualSheetName}'.",
                                                 mapping.SheetName, sourceFileName, ws.Name);
                            }
                        }
                    }
                    else
                    {
                        // No sheet name configured, use the first sheet
                        ws = wb.Worksheets.FirstOrDefault();
                        if (ws != null)
                        {
                            _logger.LogDebug("No sheet name configured. Using first worksheet: '{ActualSheetName}'.", ws.Name);
                        }
                    }

                    // Check if a worksheet was successfully selected
                    if (ws == null)
                    {
                        _logger.LogError("Could not find a suitable worksheet to process in file {FileName}. Neither configured sheet '{ConfigSheetName}' nor the first sheet was available.",
                                         sourceFileName, mapping.SheetName ?? "<Not Configured>");
                        return null; // Critical failure
                    }
                    // --- End Sheet Selection Logic ---


                    // --- Find headers and map columns ---
                    var headerRow = ws.Row(mapping.HeaderRowIndex);
                    if (headerRow.IsEmpty())
                    {
                        _logger.LogError("Header row {HeaderRowIndex} is empty in worksheet '{WorksheetName}' of file {FileName}",
                                         mapping.HeaderRowIndex, ws.Name, sourceFileName);
                        return null;
                    }

                    // MAPPING LOGIC:
                    // - Excel "Brand" -> PriceRecord.Brand -> SQL "Manufacturer"
                    // - Excel "P/N" -> PriceRecord.PartNumber -> SQL "Part_Number"
                    // - Excel "Q.TY" -> PriceRecord.Quantity -> SQL "On_Stock" 
                    // - Excel "Offer" -> PriceRecord.OfferPrice -> SQL "Price"
                    // - Excel "Uwagi" -> PriceRecord.Comment -> SQL "Comment"
                    // - PriceRecord.SourceFileName -> SQL "SourceFileName"
                    // - PriceRecord.LoadDateTime -> SQL "Import_TimeStamp"

                    // Maps PriceRecord PropertyName -> ColumnIndex
                    var colMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                    // Maps Excel Header Text -> PriceRecord PropertyName (for logging missing columns)
                    var reverseMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    foreach (var kvp in columnConfig) { reverseMap[kvp.Value] = kvp.Key; } // Excel Header -> Property Name

                    foreach (var cell in headerRow.CellsUsed(c => !string.IsNullOrWhiteSpace(c.GetString())))
                    {
                        string excelHeader = cell.GetString().Trim();
                        if (reverseMap.TryGetValue(excelHeader, out var propName))
                        {
                            if (!colMap.ContainsKey(propName)) colMap.Add(propName, cell.Address.ColumnNumber);
                            else _logger.LogWarning("Duplicate header '{HeaderText}' mapping to '{PropertyName}' found. Using first occurrence (Col {ExistingIndex}).", excelHeader, propName, colMap[propName]);
                        }
                    }
                    _logger.LogDebug("Mapped columns: {MappedCols}", string.Join(", ", colMap.Select(kv => $"{kv.Key}->Col{kv.Value}")));

                    // --- Validate Mandatory Columns ---
                    var requiredProperties = new List<string> {
                nameof(PriceRecord.Brand), nameof(PriceRecord.PartNumber),
                nameof(PriceRecord.Quantity), nameof(PriceRecord.OfferPrice)
                // Add other REQUIRED property names based on ExcelMapping.Columns keys
            };
                    var missingProps = requiredProperties.Where(p => !colMap.ContainsKey(p)).ToList();
                    if (missingProps.Any())
                    {
                        _logger.LogError("Missing required Excel columns for properties: {MissingProps} in file {SourceFileName}. Check ExcelMapping config & file headers.",
                                         string.Join(", ", missingProps), sourceFileName);
                        return null; // Cannot proceed
                    }

                    // --- Process data rows ---
                    int dataStartRow = mapping.DataStartRowIndex;
                    int lastRow = ws.LastRowUsed()?.RowNumber() ?? (dataStartRow - 1);
                    _logger.LogDebug("Processing data rows from {DataStartRow} to {LastRow}", dataStartRow, lastRow);
                    DateTime loadTimestamp = DateTime.UtcNow;

                    for (int r = dataStartRow; r <= lastRow; r++)
                    {
                        var row = ws.Row(r);
                        if (row.IsEmpty()) continue;

                        var record = new PriceRecord { LoadDateTime = loadTimestamp, SourceFileName = sourceFileName };
                        bool isValidRow = true;

                        try
                        {
                            // Populate based on mapped columns
                            record.Brand = GetStringValue(row, colMap.GetValueOrDefault(nameof(PriceRecord.Brand)));
                            record.PartNumber = GetStringValue(row, colMap.GetValueOrDefault(nameof(PriceRecord.PartNumber)));
                            record.Quantity = GetIntValue(row, colMap.GetValueOrDefault(nameof(PriceRecord.Quantity)));
                            record.OfferPrice = GetDecimalValue(row, colMap.GetValueOrDefault(nameof(PriceRecord.OfferPrice)));
                            record.Comment = GetStringValue(row, colMap.GetValueOrDefault(nameof(PriceRecord.Comment)));
                            record.Description = GetStringValue(row, colMap.GetValueOrDefault(nameof(PriceRecord.Description)));
                            // Add other property assignments here based on colMap...

                            // Basic Row Validation
                            if (string.IsNullOrWhiteSpace(record.PartNumber))
                            {
                                _logger.LogWarning("Skipping row {RowNumber} in {FileName} due to missing PartNumber (P/N).", r, sourceFileName);
                                isValidRow = false;
                            }
                            // Add more validation if needed
                        }
                        catch (Exception rowEx)
                        {
                            _logger.LogError(rowEx, "Error parsing data in row {RowNum} of file {FileName}. Skipping row.", r, sourceFileName);
                            isValidRow = false;
                        }

                        if (isValidRow) records.Add(record);

                    } // End row loop
                } // Dispose workbook
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error parsing Excel file {FileName}", sourceFileName);
                return null; // Indicate critical failure
            }

            _logger.LogInformation("Successfully parsed {Count} valid records from {FileName}", records.Count, sourceFileName);
            return records;
        }

        // --- Cell Value Helper Methods ---
        private string GetStringValue(IXLRow row, int columnIndex)
        {
            if (columnIndex <= 0) return null;
            // Use GetValue<string>() for better type handling and trimming
            var value = row.Cell(columnIndex).GetValue<string>();
            return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
        }

        private int? GetIntValue(IXLRow row, int columnIndex)
        {
            if (columnIndex <= 0) return null; // No column mapped
            var cell = row.Cell(columnIndex);
            if (cell.IsEmpty()) return null; // Cell is blank

            if (cell.TryGetValue(out int intValue)) return intValue;
            if (cell.TryGetValue(out double doubleValue) && doubleValue >= int.MinValue && doubleValue <= int.MaxValue && doubleValue == Math.Truncate(doubleValue)) return (int)doubleValue;
            if (int.TryParse(cell.GetString().Trim(), NumberStyles.Any, _cultureInfo, out int parsedInt)) return parsedInt;

            _logger.LogWarning("Cannot parse Integer from cell {CellAddress} value '{CellValue}'", cell.Address.ToStringRelative(true), cell.GetString());
            return null; // Return null if parsing fails
        }

        private decimal? GetDecimalValue(IXLRow row, int columnIndex)
        {
            if (columnIndex <= 0) return null; // No column mapped
            var cell = row.Cell(columnIndex);
            if (cell.IsEmpty()) return null; // Cell is blank

            if (cell.TryGetValue(out decimal decValue)) return decValue;
            if (cell.TryGetValue(out double doubleValue))
            {
                try { return Convert.ToDecimal(doubleValue); }
                catch (OverflowException) { _logger.LogWarning("Overflow converting Double '{DoubleValue}' to Decimal in cell {CellAddress}", doubleValue, cell.Address.ToStringRelative(true)); return null; }
            }

            var stringValue = cell.GetString().Trim();
            // Try parsing string representation with different cultures
            if (decimal.TryParse(stringValue, NumberStyles.Any, _cultureInfo, out decimal resultCult)) return resultCult; // Try specific culture first (e.g., pl-PL)
            if (decimal.TryParse(stringValue, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal resultInv)) return resultInv; // Fallback to invariant

            _logger.LogWarning("Cannot parse Decimal from cell {CellAddress} value '{CellValue}'. Tried specific and invariant cultures.", cell.Address.ToStringRelative(true), cell.GetString());
            return null; // Return null if parsing fails
        }

        // --- Database Insert Method ---
        private async Task InsertDataAsync(List<PriceRecord> records, string sourceFileSummary, CancellationToken cancellationToken)
        {
            if (records == null || !records.Any())
            {
                _logger.LogWarning("No records provided to InsertDataAsync.");
                return;
            }
            _logger.LogInformation("Preparing to insert/update database table {TableName} with {RecordCount} records.", _settings.Sql.TargetTable, records.Count);

            try
            {
                var dataTable = CreateSqlDataTable(records); // This creates the DataTable with proper SQL column names
                if (dataTable.Rows.Count == 0)
                {
                    _logger.LogWarning("CreateSqlDataTable returned 0 rows. Skipping database insert.");
                    return;
                }

                await using (var conn = new SqlConnection(_settings.Sql.ConnectionString))
                {
                    await conn.OpenAsync(cancellationToken);
                    _logger.LogDebug("SQL connection opened.");

                    string safeTableName = SanitizeTableName(_settings.Sql.TargetTable);

                    // Get actual columns from SQL table
                    var tableColumns = await GetSqlTableColumnsAsync(conn, safeTableName, cancellationToken);

                    _logger.LogWarning("Truncating table {TableName} before inserting new data.", safeTableName);
                    await using (var cmd = new SqlCommand($"TRUNCATE TABLE {safeTableName}", conn))
                    {
                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }
                    _logger.LogInformation("Table {TableName} truncated successfully.", safeTableName);

                    _logger.LogDebug("Starting SqlBulkCopy to {TableName}...", safeTableName);
                    using (var bulk = new SqlBulkCopy(conn))
                    {
                        bulk.DestinationTableName = safeTableName;
                        bulk.BatchSize = 5000;
                        bulk.BulkCopyTimeout = 120;

                        // Map DataTable columns (already in SQL format) to the SQL table columns
                        // Example mapping: "Manufacturer" -> "Manufacturer"
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            // Check if the column exists in the SQL table (case-insensitive)
                            if (tableColumns.Any(c => string.Equals(c, column.ColumnName, StringComparison.OrdinalIgnoreCase)))
                            {
                                // Find the exact case of the column name in the SQL table
                                string exactColumnName = tableColumns.First(c =>
                                    string.Equals(c, column.ColumnName, StringComparison.OrdinalIgnoreCase));

                                _logger.LogDebug("Adding column mapping: {SourceColumn} -> {DestColumn}",
                                    column.ColumnName, exactColumnName);
                                bulk.ColumnMappings.Add(column.ColumnName, exactColumnName);
                            }
                            else
                            {
                                _logger.LogWarning("Column {Column} exists in DataTable but not in SQL table - skipping mapping",
                                    column.ColumnName);
                            }
                        }

                        _logger.LogDebug("SqlBulkCopy configured with {MappingCount} column mappings", bulk.ColumnMappings.Count);

                        if (bulk.ColumnMappings.Count == 0)
                        {
                            throw new InvalidOperationException("No valid column mappings found between DataTable and SQL table. Check column names.");
                        }

                        await bulk.WriteToServerAsync(dataTable, cancellationToken);
                        _logger.LogInformation("Successfully inserted {Count} rows into {TableName}.", records.Count, safeTableName);
                    } // Dispose SqlBulkCopy
                } // Dispose Connection
            }
            catch (SqlException sqlEx)
            {
                _logger.LogCritical(sqlEx, "SQL Error during database update. Error Number: {Number}", sqlEx.Number);
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Generic error during database update process.");
                throw;
            }
        }

        // Add this method to your DataProcessor class

        private async Task<List<string>> GetSqlTableColumnsAsync(SqlConnection connection, string tableName, CancellationToken cancellationToken)
        {
            var columns = new List<string>();

            string schema = "dbo"; // Default schema
            string table = tableName;

            // Handle schema.table format
            if (tableName.Contains('.'))
            {
                var parts = tableName.Trim('[', ']').Split('.');
                if (parts.Length == 2)
                {
                    schema = parts[0].Trim('[', ']');
                    table = parts[1].Trim('[', ']');
                }
            }

            _logger.LogDebug("Getting column information for table {Schema}.{Table}", schema, table);

            string query = @"
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @Table
        ORDER BY ORDINAL_POSITION";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@Schema", schema);
                cmd.Parameters.AddWithValue("@Table", table);

                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    columns.Add(reader.GetString(0));
                }

                _logger.LogInformation("Found {Count} columns in table {Schema}.{Table}: {Columns}",
                    columns.Count, schema, table, string.Join(", ", columns));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving column information for table {Schema}.{Table}", schema, table);
            }

            return columns;
        }




        // --- HELPER: Create DataTable matching SQL structure ---
        private DataTable CreateSqlDataTable(List<PriceRecord> records)
        {
            var dataTable = new DataTable();

            // Define columns EXACTLY matching the SQL table columns
            dataTable.Columns.Add("Manufacturer", typeof(string));     // Target for record.Brand (Excel "Brand")
            dataTable.Columns.Add("Part_Number", typeof(string));      // Target for record.PartNumber (Excel "P/N")
            dataTable.Columns.Add("On_Stock", typeof(int));            // Target for record.Quantity (Excel "Q.TY")
            dataTable.Columns.Add("Price", typeof(decimal));           // Target for record.OfferPrice (Excel "Offer")
            dataTable.Columns.Add("SourceFileName", typeof(string));   // Target for record.SourceFileName
            dataTable.Columns.Add("Import_TimeStamp", typeof(DateTime));// Target for record.LoadDateTime
            dataTable.Columns.Add("Description", typeof(string)).AllowDBNull = true;

            // Only add Comment column if it's actually going to be mapped
            try
            {
                // This is optional - only include if Comment column exists in SQL table
                dataTable.Columns.Add("Comment", typeof(string)).AllowDBNull = true; // From Excel "Uwagi"
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error adding Comment column to DataTable. It may not exist in the SQL table.");
            }

            // Set AllowDBNull based on SQL table definition
            dataTable.Columns["Manufacturer"].AllowDBNull = true;
            dataTable.Columns["Part_Number"].AllowDBNull = false;
            dataTable.Columns["On_Stock"].AllowDBNull = true;
            dataTable.Columns["Price"].AllowDBNull = true;
            dataTable.Columns["SourceFileName"].AllowDBNull = true;
            dataTable.Columns["Import_TimeStamp"].AllowDBNull = false;

            // Log the column schema
            _logger.LogDebug("DataTable created with columns: {Columns}", string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));

            // Populate DataTable rows, mapping from PriceRecord properties to SQL column names
            foreach (var record in records)
            {
                var row = dataTable.NewRow();

                // Explicit mapping of PriceRecord fields to SQL table columns
                row["Manufacturer"] = (object)record.Brand ?? DBNull.Value;           // From Excel "Brand"
                row["Part_Number"] = (object)record.PartNumber ?? DBNull.Value;       // From Excel "P/N"
                row["On_Stock"] = (object)record.Quantity ?? DBNull.Value;            // From Excel "Q.TY"
                row["Price"] = (object)record.OfferPrice ?? DBNull.Value;             // From Excel "Offer"
                row["SourceFileName"] = (object)record.SourceFileName ?? DBNull.Value;
                row["Import_TimeStamp"] = (object)record.LoadDateTime ?? DBNull.Value;
                row["Description"] = (object)record.Description ?? DBNull.Value;

                // Only add Comment if the column exists
                if (dataTable.Columns.Contains("Comment"))
                    row["Comment"] = (object)record.Comment ?? DBNull.Value;          // From Excel "Uwagi"

                dataTable.Rows.Add(row);
            }

            _logger.LogDebug("Created DataTable for SQL with {RowCount} rows and {ColumnCount} columns.", dataTable.Rows.Count, dataTable.Columns.Count);
            return dataTable;
        }

        // --- Keep SanitizeTableName helper ---
        private string SanitizeTableName(string tableName)
        {
            string name = tableName?.Replace("[", "").Replace("]", "");
            if (string.IsNullOrWhiteSpace(name) || !name.Contains('.')) return tableName; // Basic check
            var parts = name.Split('.');
            return $"[{parts[0]}].[{parts[1]}]";
        }

        // --- SharePoint/Graph Helper Methods (Keep from previous version) ---
        private async Task<string> GetSiteIdAsync(string siteNameOrPath, CancellationToken cancellationToken)
        {
            // In SDK v5, accessing by hostname is preferred if possible & permissions allow (Sites.Read.All)
            _logger.LogDebug("Attempting to get Site ID using identifier: {SiteIdentifier}", siteNameOrPath);
            try
            {
                // Use the configured SiteName directly. Assumes it's a valid identifier (hostname or server-relative path)
                var site = await _graphClient.Sites[siteNameOrPath]
                    .GetAsync(req => req.QueryParameters.Select = new[] { "id", "displayName", "webUrl" }, cancellationToken);

                if (site != null)
                {
                    _logger.LogInformation("Found Site: {DisplayName} ({WebUrl}) - ID: {SiteId}", site.DisplayName, site.WebUrl, site.Id);
                    return site.Id;
                }
                _logger.LogError("GetSiteIdAsync returned null for identifier '{SiteIdentifier}'.", siteNameOrPath);
                return null; // Should not happen if no exception
            }
            catch (ODataError oex) when (oex.ResponseStatusCode == (int)HttpStatusCode.NotFound)
            {
                _logger.LogError(oex, "Site not found using identifier '{SiteIdentifier}'. Check AppSettings:SharePoint:SiteName.", siteNameOrPath);
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving Site ID using identifier '{SiteIdentifier}'.", siteNameOrPath);
                throw; // Re-throw unexpected errors
            }
        }

        private async Task<DriveItem> GetFolderItemByPathAsync(string driveId, string relativeFolderPath, CancellationToken cancellationToken, bool createIfNotExists = false)
        {
            if (string.IsNullOrEmpty(driveId) || string.IsNullOrEmpty(relativeFolderPath)) return null;

            // Graph path needs encoding and NO leading slash for ItemWithPath from root
            string encodedPath = Uri.EscapeDataString(relativeFolderPath.TrimStart('/')).Replace("%2F", "/");
            _logger.LogDebug("Getting folder item by encoded path: '{EncodedPath}'", encodedPath);

            try
            {
                var folderItem = await _graphClient.Drives[driveId].Root
                    .ItemWithPath(encodedPath)
                    .GetAsync(req => req.QueryParameters.Select = new[] { "id", "name", "folder" }, cancellationToken);

                if (folderItem?.Folder == null)
                {
                    _logger.LogError("Path '{RelativePath}' exists but is not a folder.", relativeFolderPath);
                    return null;
                }
                _logger.LogInformation("Found folder '{FolderName}' with ID: {FolderId}", folderItem.Name, folderItem.Id);
                return folderItem;
            }
            catch (ODataError ex) when (ex.ResponseStatusCode == (int)HttpStatusCode.NotFound)
            {
                _logger.LogWarning("Folder not found at path: '{RelativePath}'. CreateIfNotExists={Create}", relativeFolderPath, createIfNotExists);
                if (createIfNotExists)
                {
                    // Simplified creation: Assume parent exists and create the last segment
                    string parentPath = Path.GetDirectoryName(relativeFolderPath)?.Replace("\\", "/") ?? "";
                    string folderName = Path.GetFileName(relativeFolderPath);

                    if (string.IsNullOrEmpty(folderName))
                    {
                        _logger.LogError("Cannot determine folder name from path '{RelativePath}' for creation.", relativeFolderPath);
                        return null;
                    }

                    _logger.LogInformation("Attempting to create folder '{FolderName}' within parent path '{ParentPath}'", folderName, parentPath);
                    try
                    {
                        DriveItem parentItem;
                        if (string.IsNullOrEmpty(parentPath)) // Creating under root
                        {
                            parentItem = await _graphClient.Drives[driveId].Root.GetAsync(req => req.QueryParameters.Select = new[] { "id" }, cancellationToken);
                        }
                        else // Need to get parent item
                        {
                            string encodedParentPath = Uri.EscapeDataString(parentPath.TrimStart('/')).Replace("%2F", "/");
                            parentItem = await _graphClient.Drives[driveId].Root.ItemWithPath(encodedParentPath)
                                            .GetAsync(req => req.QueryParameters.Select = new[] { "id", "folder" }, cancellationToken);
                            if (parentItem?.Folder == null)
                            {
                                _logger.LogError("Parent path '{ParentPath}' is not a folder or doesn't exist. Cannot create '{FolderName}'.", parentPath, folderName);
                                return null;
                            }
                        }

                        if (parentItem?.Id == null)
                        {
                            _logger.LogError("Could not determine parent item ID for '{ParentPath}'. Cannot create '{FolderName}'.", parentPath, folderName);
                            return null;
                        }

                        var driveItemToCreate = new DriveItem { Name = folderName, Folder = new Folder { } };
                        var createdItem = await _graphClient.Drives[driveId].Items[parentItem.Id].Children
                                            .PostAsync(driveItemToCreate, cancellationToken: cancellationToken);
                        _logger.LogInformation("Successfully created folder '{FolderName}' with ID {FolderId}", createdItem?.Name, createdItem?.Id);
                        return createdItem;
                    }
                    catch (Exception createEx)
                    {
                        _logger.LogError(createEx, "Failed to create folder '{RelativePath}'.", relativeFolderPath);
                        return null;
                    }
                }
                return null; // Folder not found and not creating
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting folder item by path '{RelativePath}'.", relativeFolderPath);
                return null;
            }
        }

        private async Task<bool> MoveFileAsync(string driveId, DriveItem fileItem, string targetFolderId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(driveId) || fileItem?.Id == null || string.IsNullOrEmpty(targetFolderId))
            {
                _logger.LogError("MoveFileAsync: Invalid parameters provided.");
                return false;
            }

            _logger.LogInformation("Moving file '{FileName}' (ID: {FileId}) to folder ID: {TargetFolderId}", fileItem.Name, fileItem.Id, targetFolderId);

            var requestBody = new DriveItem
            {
                ParentReference = new ItemReference { Id = targetFolderId },
                AdditionalData = new Dictionary<string, object>
                 { { "@microsoft.graph.conflictBehavior", "rename" } } // e.g., rename if exists
            };

            try
            {
                await _graphClient.Drives[driveId].Items[fileItem.Id]
                        .PatchAsync(requestBody, cancellationToken: cancellationToken);
                _logger.LogInformation("Successfully moved file '{FileName}' to folder {TargetFolderId}.", fileItem.Name, targetFolderId);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to move file '{FileName}' (ID: {FileId}) to folder {TargetFolderId}.", fileItem.Name, fileItem.Id, targetFolderId);
                return false;
            }
        }

        // IMPORTANT: Ensure the PriceRecord class definition is ONLY in Program.cs now

    } // End DataProcessor Class
} // End Namespace
