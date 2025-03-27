defmodule Kylix.Benchmark.ResultVisualizer do
  @moduledoc """
  Provides visualization capabilities for Kylix benchmark results.
  """

  @output_dir "data/benchmark"

  def visualize_results(result_file) do
    # Load result data from file
    file_path = Path.join(@output_dir, result_file)

    case File.read(file_path) do
      {:ok, content} ->
        data = Jason.decode!(content)
        # Generate HTML visualization
        html = generate_visualization_html(data)
        # Save HTML to a file
        html_file = String.replace(result_file, ".json", ".html")
        html_path = Path.join(@output_dir, html_file)
        File.write!(html_path, html)
        IO.puts("Visualization saved to: #{html_path}")
        {:ok, html_path}

      {:error, reason} ->
        IO.puts("Error reading result file: #{reason}")
        {:error, reason}
    end
  end

  def list_result_files do
    # List all JSON result files in the output directory
    case File.ls(@output_dir) do
      {:ok, files} ->
        json_files = Enum.filter(files, &String.ends_with?(&1, ".json"))
        IO.puts("Available result files:")
        Enum.each(json_files, &IO.puts("  #{&1}"))
        {:ok, json_files}

      {:error, reason} ->
        IO.puts("Error listing files: #{reason}")
        {:error, reason}
    end
  end

  def generate_visualization_data(results) do
    # Generate data for various charts based on the results
    # Similar to generate_visualization_data_json in original code
    %{
      baseline: %{
        title: "Transaction Processing",
        tps: results.transactions_per_second,
        avg_latency: results.average_tx_time_us,
        transaction_times: results.transaction_times
      }
      # Other visualization data would be included here
    }
  end

  def generate_visualization_html(data) do
    # Generate HTML with Chart.js for visualizing the data
    # This would be a simplified version of the original HTML
"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kylix Benchmark Results</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.7.1/chart.min.js"></script>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #333;
            text-align: center;
        }
        .chart-container {
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 20px;
            margin-bottom: 30px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Kylix Benchmark Results</h1>
        <div class="chart-container">
            <canvas id="tpsChart"></canvas>
        </div>
        <div class="chart-container">
            <canvas id="latencyChart"></canvas>
        </div>
    </div>

    <script>
        // JavaScript code to render charts using the data
        const data = #{Jason.encode!(data)};

        // Create TPS chart
        const tpsCtx = document.getElementById('tpsChart').getContext('2d');
        new Chart(tpsCtx, {
            type: 'bar',
            data: {
                labels: ['Transactions Per Second'],
                datasets: [{
                    label: 'TPS',
                    data: [data.transactions_per_second],
                    backgroundColor: 'rgba(54, 162, 235, 0.7)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });

        // Create latency chart
        const latencyCtx = document.getElementById('latencyChart').getContext('2d');
        new Chart(latencyCtx, {
            type: 'line',
            data: {
                labels: Array.from({length: data.transaction_times.length}, (_, i) => i + 1),
                datasets: [{
                    label: 'Transaction Time (μs)',
                    data: data.transaction_times,
                    borderColor: 'rgba(255, 99, 132, 1)',
                    backgroundColor: 'rgba(255, 99, 132, 0.1)',
                    borderWidth: 2,
                    fill: true
                }]
            },
            options: {
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Time (μs)'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Transaction #'
                        }
                    }
                }
            }
        });
    </script>
</body>
</html>
"""
  end
end
