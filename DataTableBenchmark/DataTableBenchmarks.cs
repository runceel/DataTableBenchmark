using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System.Data;

namespace DataTableBenchmark;

[SimpleJob(runtimeMoniker: RuntimeMoniker.Net48, baseline: true)]
[SimpleJob(runtimeMoniker: RuntimeMoniker.Net60)]
[MemoryDiagnoser]
public class DataTableBenchmarks
{
    private int _rowCount = 100000;
    private int _columnCount = 100;
    private DataTable _dataTable;

    [IterationSetup(Targets = new[] { nameof(SelectMethod), nameof(LinqToDataSet), nameof(CreateIndexAndSelectMethod) })]
    public void Setup()
    {
        _dataTable = CreateDataTable();
    }

    [IterationSetup(Target = nameof(SelectMethodWithIndex))]
    public void SetupForIndex()
    {
        _dataTable = CreateDataTable();
        _dataTable.DefaultView.Sort = "COLUMN_0, COLUMN_1";
    }

    private DataTable CreateDataTable()
    {
        var dataTable = new DataTable("DummyTable");
        // ダミー列の作成 COLUMN_0 〜 COLUMN_ColumnCountまで作る
        foreach (var column in Enumerable.Range(0, _columnCount))
        {
            dataTable.Columns.Add($"COLUMN_{column}");
        }

        // ダミーデータの作成
        foreach (var row in Enumerable.Range(0, _rowCount))
        {
            var rowData = dataTable.NewRow();
            foreach (var column in Enumerable.Range(0, _columnCount))
            {
                rowData[column] = $"DATA_{(row + 1) * (column + 1) % 100}";
            }
            dataTable.Rows.Add(rowData);
        }

        return dataTable;
    }

    [Benchmark]
    public DataRow[] SelectMethod() =>
        _dataTable.Select("COLUMN_0 = 'DATA_10' OR COLUMN_1 like 'DATA_1%'", "COLUMN_0");

    [Benchmark]
    public DataRow[] CreateIndexAndSelectMethod()
    {
        _dataTable.DefaultView.Sort = "COLUMN_0, COLUMN_1";
        return _dataTable.Select("COLUMN_0 = 'DATA_10' OR COLUMN_1 like 'DATA_1%'", "COLUMN_0");
    }

    [Benchmark]
    public DataRow[] SelectMethodWithIndex() =>
        _dataTable.Select("COLUMN_0 = 'DATA_10' OR COLUMN_1 like 'DATA_1%'", "COLUMN_0");

    [Benchmark]
    public DataRow[] LinqToDataSet() =>
        _dataTable.AsEnumerable()
            .Where(x => x.Field<string>("COLUMN_0") == "DATA_10" && x.Field<string>("COLUMN_1").StartsWith("DATA_1"))
            .ToArray();
}
