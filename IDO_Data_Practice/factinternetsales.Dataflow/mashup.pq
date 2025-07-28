[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactInternetSales_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared FactInternetSales = let
  Source = Excel.Workbook(Web.Contents("https://talentship365-my.sharepoint.com/personal/lakshmi_prabha_talentship_io/Documents/Apps/Microsoft Power Query/Uploaded Files/FabricData 6.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "FactInternetSales", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductKey", Int64.Type}, {"OrderDateKey", Int64.Type}, {"DueDateKey", Int64.Type}, {"ShipDateKey", Int64.Type}, {"CustomerKey", Int64.Type}, {"PromotionKey", Int64.Type}, {"CurrencyKey", Int64.Type}, {"SalesTerritoryKey", Int64.Type}, {"SalesOrderNumber", type text}, {"SalesOrderLineNumber", Int64.Type}, {"RevisionNumber", Int64.Type}, {"OrderQuantity", Int64.Type}, {"UnitPrice", type number}, {"ExtendedAmount", type number}, {"UnitPriceDiscountPct", Int64.Type}, {"DiscountAmount", Int64.Type}, {"ProductStandardCost", type number}, {"TotalProductCost", type number}, {"SalesAmount", type number}, {"TaxAmt", type number}, {"Freight", type number}, {"CarrierTrackingNumber", type text}, {"CustomerPONumber", type text}, {"OrderDate", type date}, {"DueDate", type date}, {"ShipDate", type date}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"OrderDateKey", "DueDateKey", "ShipDateKey", "CustomerKey", "PromotionKey", "CurrencyKey", "SalesTerritoryKey", "SalesOrderLineNumber", "RevisionNumber", "OrderQuantity", "UnitPrice", "ExtendedAmount", "UnitPriceDiscountPct", "DiscountAmount", "ProductStandardCost", "TotalProductCost", "TaxAmt", "Freight", "CarrierTrackingNumber", "CustomerPONumber", "OrderDate", "DueDate", "ShipDate"})
in
  #"Removed columns";
shared FactInternetSales_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "63f70da2-ae03-49e1-9d3e-f7e0d1980727"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5ff92c5e-15c1-4f8e-ac83-4f040a3944d9"]}[Data],
  TableNavigation = Navigation_2{[Id = "FactInternetSales", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
