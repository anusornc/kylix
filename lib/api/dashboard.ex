defmodule Kylix.API.Dashboard do
  @moduledoc """
  Simple HTML dashboard for Kylix blockchain explorer
  """

  def render do
    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Kylix Blockchain Explorer</title>
      <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
      <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
      <style>
        /* Tooltip styles */
        .tooltip {
          position: relative;
          cursor: pointer;
        }
        .tooltip:hover::after {
          content: attr(title);
          position: absolute;
          bottom: 100%;
          left: 50%;
          transform: translateX(-50%);
          background-color: #333;
          color: white;
          padding: 5px 10px;
          border-radius: 4px;
          white-space: nowrap;
          z-index: 10;
        }
      </style>
    </head>
    <body class="bg-gray-100">
      <div class="container mx-auto px-4 py-8">
        <header class="mb-8">
          <h1 class="text-3xl font-bold text-gray-800">Kylix Blockchain Explorer</h1>
          <p class="text-gray-600">A DAG-based blockchain for provenance tracking</p>
        </header>

        <!-- 1. Transaction Explorer Section - Now full width -->
        <div class="bg-white p-6 rounded-lg shadow-md mb-8">
          <h2 class="text-xl font-semibold mb-4">Transaction Explorer</h2>
          <div class="mb-4 flex flex-wrap gap-4">
            <input id="txSearch" type="text" placeholder="Search by ID, subject, predicate, or object"
              class="flex-grow p-2 border border-gray-300 rounded">
            <button id="loadTxBtn" class="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600">
              Load Transactions
            </button>
            <select id="txSortOrder" class="p-2 border border-gray-300 rounded">
              <option value="id_asc">ID (Ascending)</option>
              <option value="id_desc">ID (Descending)</option>
              <option value="timestamp_desc" selected>Newest First</option>
              <option value="timestamp_asc">Oldest First</option>
            </select>
          </div>
          <div id="txResults" class="mt-4 overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead>
                <tr>
                  <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
                  <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Subject</th>
                  <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Predicate</th>
                  <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Object</th>
                  <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Validator</th>
                  <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Timestamp</th>
                  <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Hash</th>
                </tr>
              </thead>
              <tbody id="txTableBody" class="divide-y divide-gray-200">
                <!-- Transaction rows will go here -->
              </tbody>
            </table>
          </div>
        </div>

        <!-- 2. SPARQL Query Section - Now full width -->
        <div class="bg-white p-6 rounded-lg shadow-md mb-8">
          <h2 class="text-xl font-semibold mb-4">SPARQL Query</h2>
          <div class="mb-4">
            <textarea id="sparqlQuery" rows="5" placeholder="Enter SPARQL query..."
              class="w-full p-2 border border-gray-300 rounded"></textarea>
          </div>
          <div class="mb-4">
            <button id="runQueryBtn" class="bg-green-500 text-white px-4 py-2 rounded hover:bg-green-600">
              Run Query
            </button>
          </div>
          <div id="queryResults" class="mt-4 overflow-x-auto">
            <!-- Query results will go here -->
          </div>
        </div>

        <!-- 3. Performance Benchmarks Section - Now full width -->
        <div class="bg-white p-6 rounded-lg shadow-md mb-8">
          <h2 class="text-xl font-semibold mb-4">Performance Benchmarks</h2>

          <div class="mb-6 p-4 bg-gray-50 rounded-lg">
            <h3 class="font-medium text-gray-700 mb-3">Run Transaction Speed Test</h3>
            <div class="flex flex-wrap gap-4 items-end">
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Transaction Count</label>
                <input type="number" id="benchmarkCount" min="10" max="10000" value="100" class="p-2 border border-gray-300 rounded w-32">
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Concurrent Connections</label>
                <input type="number" id="benchmarkConcurrent" min="1" max="100" value="4" class="p-2 border border-gray-300 rounded w-32">
              </div>
              <div>
                <button id="runBenchmarkBtn" class="bg-purple-600 text-white px-4 py-2 rounded hover:bg-purple-700">
                  Run Benchmark
                </button>
              </div>
            </div>
            <div id="benchmarkStatus" class="mt-3 text-sm hidden">
              <div class="flex items-center">
                <svg class="animate-spin h-5 w-5 mr-2 text-purple-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                  <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                <span>Running benchmark... This may take a few minutes</span>
              </div>
            </div>
          </div>

          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <!-- Transaction Throughput -->
            <div>
              <h3 class="font-medium text-gray-700 mb-2">Transaction Throughput</h3>
              <div class="border rounded p-4 h-64">
                <canvas id="txThroughputChart"></canvas>
              </div>
              <div class="mt-2 grid grid-cols-2 gap-2 text-sm">
                <div class="bg-gray-100 p-2 rounded">
                  <div class="font-medium">Tx/sec</div>
                  <div id="txPerSecond" class="text-lg">-</div>
                </div>
                <div class="bg-gray-100 p-2 rounded">
                  <div class="font-medium">Total Time</div>
                  <div id="totalTime" class="text-lg">-</div>
                </div>
              </div>
            </div>

            <!-- Latency Distribution -->
            <div>
              <h3 class="font-medium text-gray-700 mb-2">Latency Distribution</h3>
              <div class="border rounded p-4 h-64">
                <canvas id="latencyDistChart"></canvas>
              </div>
              <div class="mt-2 grid grid-cols-2 gap-2 text-sm">
                <div class="bg-gray-100 p-2 rounded">
                  <div class="font-medium">Avg Latency</div>
                  <div id="avgLatency" class="text-lg">-</div>
                </div>
                <div class="bg-gray-100 p-2 rounded">
                  <div class="font-medium">95% Latency</div>
                  <div id="p95Latency" class="text-lg">-</div>
                </div>
              </div>
            </div>

            <!-- Cache Performance -->
            <div>
              <h3 class="font-medium text-gray-700 mb-2">Cache Performance</h3>
              <div class="border rounded p-4 h-64">
                <canvas id="cacheChart"></canvas>
              </div>
              <div class="mt-2 grid grid-cols-2 gap-2 text-sm">
                <div class="bg-gray-100 p-2 rounded">
                  <div class="font-medium">Hit Rate</div>
                  <div id="cacheHitRate" class="text-lg">-</div>
                </div>
                <div class="bg-gray-100 p-2 rounded">
                  <div class="font-medium">Cache Size</div>
                  <div id="cacheSize" class="text-lg">-</div>
                </div>
              </div>
            </div>

            <!-- Storage Metrics -->
            <div>
              <h3 class="font-medium text-gray-700 mb-2">Storage Metrics</h3>
              <div class="border rounded p-4 h-64">
                <canvas id="storageChart"></canvas>
              </div>
              <div class="mt-2 grid grid-cols-2 gap-2 text-sm">
                <div class="bg-gray-100 p-2 rounded">
                  <div class="font-medium">Nodes</div>
                  <div id="nodeCount" class="text-lg">-</div>
                </div>
                <div class="bg-gray-100 p-2 rounded">
                  <div class="font-medium">Edges</div>
                  <div id="edgeCount" class="text-lg">-</div>
                </div>
              </div>
            </div>
          </div>

          <!-- Benchmark History -->
          <div class="mt-6">
            <h3 class="font-medium text-gray-700 mb-2">Benchmark History</h3>
            <div class="overflow-x-auto">
              <table class="min-w-full divide-y divide-gray-200">
                <thead>
                  <tr>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Tx Count</th>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Concurrent</th>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Tx/sec</th>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Avg Latency</th>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">p95 Latency</th>
                  </tr>
                </thead>
                <tbody id="benchmarkHistory" class="divide-y divide-gray-200">
                  <!-- Benchmark history rows will go here -->
                </tbody>
              </table>
            </div>
          </div>

          <div class="mt-4 text-right">
            <button id="refreshBenchmarksBtn" class="bg-indigo-500 text-white px-4 py-2 rounded hover:bg-indigo-600">
              Refresh Metrics
            </button>
          </div>
        </div>

        <!-- 4. Transaction Submission Form - Now full width -->
        <div class="bg-white p-6 rounded-lg shadow-md mb-8">
          <h2 class="text-xl font-semibold mb-4">Submit Transaction</h2>
          <form id="txForm" class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">Subject</label>
              <input type="text" id="txSubject" class="w-full p-2 border border-gray-300 rounded">
            </div>
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">Predicate</label>
              <input type="text" id="txPredicate" class="w-full p-2 border border-gray-300 rounded">
            </div>
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">Object</label>
              <input type="text" id="txObject" class="w-full p-2 border border-gray-300 rounded">
            </div>
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">Validator ID</label>
              <select id="txValidator" class="w-full p-2 border border-gray-300 rounded">
                <!-- Validators will be loaded here -->
              </select>
            </div>
            <div class="md:col-span-2">
              <label class="block text-sm font-medium text-gray-700 mb-1">Signature (use "valid_sig" for testing)</label>
              <input type="text" id="txSignature" value="valid_sig" class="w-full p-2 border border-gray-300 rounded">
            </div>
            <div class="md:col-span-3">
              <button type="submit" class="bg-purple-500 text-white px-4 py-2 rounded hover:bg-purple-600">
                Submit Transaction
              </button>
            </div>
          </form>
        </div>

        <!-- 5. Validator Metrics Section -->
        <div class="bg-white p-6 rounded-lg shadow-md mb-8">
        <h2 class="text-xl font-semibold mb-4">Validator Metrics</h2>

        <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Validator</th>
              <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Total Txs</th>
              <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Success Rate</th>
              <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Avg Tx Time</th>
              <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Active</th>
            </tr>
          </thead>
          <tbody id="validatorMetricsBody" class="divide-y divide-gray-200">
            <!-- Validator metrics will go here -->
          </tbody>
        </table>
        </div>

        <div class="mt-4">
        <h3 class="font-medium text-gray-700 mb-2">Current Validator</h3>
        <div id="currentValidator" class="p-2 bg-green-100 rounded inline-block font-medium">Loading...</div>
        </div>
        </div>
      </div>

      <script>
    // JavaScript to interact with the API
    document.addEventListener('DOMContentLoaded', function() {
    // Load validators for the dropdown
    fetch('/validators')
      .then(response => response.json())
      .then(data => {
        if (data.status === 'success') {
          const validatorSelect = document.getElementById('txValidator');
          data.data.forEach(validator => {
            const option = document.createElement('option');
            option.value = validator;
            option.textContent = validator;
            validatorSelect.appendChild(option);
          });
        }
      })
      .catch(error => console.error('Error loading validators:', error));

    // Load transactions button handler
    document.getElementById('loadTxBtn').addEventListener('click', function() {
      loadTransactions();
    });

    // Sort order change
    document.getElementById('txSortOrder').addEventListener('change', function() {
      loadTransactions();
    });

    function loadTransactions() {
      fetch('/transactions')
        .then(response => response.json())
        .then(data => {
          if (data.status === 'success') {
            const tableBody = document.getElementById('txTableBody');
            tableBody.innerHTML = '';

            // Get sort order
            const sortOrder = document.getElementById('txSortOrder').value;
            let sortedData = [...data.data];

            // Apply sorting
            switch(sortOrder) {
              case 'id_asc':
                sortedData.sort((a, b) => a.id.localeCompare(b.id));
                break;
              case 'id_desc':
                sortedData.sort((a, b) => b.id.localeCompare(a.id));
                break;
              case 'timestamp_asc':
                sortedData.sort((a, b) => new Date(a.timestamp || 0) - new Date(b.timestamp || 0));
                break;
              case 'timestamp_desc':
                sortedData.sort((a, b) => new Date(b.timestamp || 0) - new Date(a.timestamp || 0));
                break;
            }

            sortedData.forEach(tx => {
              const row = document.createElement('tr');
              // Convert timestamp to a more readable format
              const timestamp = tx.timestamp ? new Date(tx.timestamp).toLocaleString() : 'N/A';
              // Truncate hash for display
              const hashDisplay = tx.hash ? tx.hash.substring(0, 12) + '...' : 'N/A';
              const fullHash = tx.hash || 'N/A';

              row.innerHTML = `
                <td class="px-4 py-2">${tx.id}</td>
                <td class="px-4 py-2">${tx.subject}</td>
                <td class="px-4 py-2">${tx.predicate}</td>
                <td class="px-4 py-2">${tx.object}</td>
                <td class="px-4 py-2">${tx.validator || 'N/A'}</td>
                <td class="px-4 py-2">${timestamp}</td>
                <td class="px-4 py-2 tooltip" title="${fullHash}">${hashDisplay}</td>
              `;
              tableBody.appendChild(row);
            });
          } else {
            alert('Error loading transactions: ' + data.message);
          }
        })
        .catch(error => console.error('Error loading transactions:', error));
    }

    // Run query button handler
    document.getElementById('runQueryBtn').addEventListener('click', function() {
      const query = document.getElementById('sparqlQuery').value.trim();
      if (!query) {
        alert('Please enter a SPARQL query');
        return;
      }

      fetch(`/query?q=${encodeURIComponent(query)}`)
        .then(response => response.json())
        .then(data => {
          const resultsDiv = document.getElementById('queryResults');

          if (data.status === 'success') {
            // Create a table for the results
            let html = '<table class="min-w-full divide-y divide-gray-200">';

            // Extract headers from first result
            if (data.data.length > 0) {
              const headers = Object.keys(data.data[0]);
              html += '<thead><tr>';
              headers.forEach(header => {
                html += `<th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">${header}</th>`;
              });
              html += '</tr></thead>';

              // Add rows
              html += '<tbody>';
              data.data.forEach(row => {
                html += '<tr>';
                headers.forEach(header => {
                  const value = row[header] !== null ? row[header] : '';
                  html += `<td class="px-4 py-2">${value}</td>`;
                });
                html += '</tr>';
              });
              html += '</tbody>';
            } else {
              html += '<p>No results found</p>';
            }

            html += '</table>';
            resultsDiv.innerHTML = html;
          } else {
            resultsDiv.innerHTML = `<p class="text-red-500">Error: ${data.message}</p>`;
          }
        })
        .catch(error => {
          console.error('Error running query:', error);
          document.getElementById('queryResults').innerHTML =
            `<p class="text-red-500">Error: ${error.message}</p>`;
        });
    });

    // Submit transaction form handler
    document.getElementById('txForm').addEventListener('submit', function(e) {
      e.preventDefault();

      const txData = {
        subject: document.getElementById('txSubject').value,
        predicate: document.getElementById('txPredicate').value,
        object: document.getElementById('txObject').value,
        validator_id: document.getElementById('txValidator').value,
        signature: document.getElementById('txSignature').value
      };

      // Validate inputs
      if (!txData.subject || !txData.predicate || !txData.object || !txData.validator_id) {
        alert('Please fill in all required fields');
        return;
      }

      fetch('/transactions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(txData)
      })
        .then(response => response.json())
        .then(data => {
          if (data.status === 'success') {
            alert(`Transaction submitted successfully with ID: ${data.transaction_id}`);
            // Clear form
            document.getElementById('txSubject').value = '';
            document.getElementById('txPredicate').value = '';
            document.getElementById('txObject').value = '';

            // Refresh data
            loadTransactions();
            loadBenchmarkData();
          } else {
            alert('Error submitting transaction: ' + data.message);
          }
        })
        .catch(error => console.error('Error submitting transaction:', error));
    });

    // Run benchmark button handler
    document.getElementById('runBenchmarkBtn').addEventListener('click', function() {
      const count = parseInt(document.getElementById('benchmarkCount').value);
      const concurrent = parseInt(document.getElementById('benchmarkConcurrent').value);

      if (isNaN(count) || count < 10 || isNaN(concurrent) || concurrent < 1) {
        alert('Please enter valid benchmark parameters');
        return;
      }

      // Show loading indicator
      document.getElementById('benchmarkStatus').classList.remove('hidden');
      document.getElementById('runBenchmarkBtn').disabled = true;

      // Call the API to run the benchmark
      fetch('/run-benchmark', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          count: count,
          concurrent: concurrent
        })
      })
      .then(response => response.json())
      .then(data => {
        // Hide loading indicator
        document.getElementById('benchmarkStatus').classList.add('hidden');
        document.getElementById('runBenchmarkBtn').disabled = false;

        if (data.status === 'success') {
          // Display success message
          alert('Benchmark completed successfully!');

          // Refresh benchmark data to show the new results
          loadBenchmarkData();
        } else {
          alert('Error running benchmark: ' + data.message);
        }
      })
      .catch(error => {
        // Hide loading indicator
        document.getElementById('benchmarkStatus').classList.add('hidden');
        document.getElementById('runBenchmarkBtn').disabled = false;

        console.error('Error running benchmark:', error);
        alert('Error running benchmark: ' + error.message);
      });
    });

    // Initialize benchmark charts
    initBenchmarkCharts();

    // Load initial data
    loadTransactions();
    loadBenchmarkData();

    // Refresh benchmarks button handler
    document.getElementById('refreshBenchmarksBtn').addEventListener('click', function() {
      loadBenchmarkData();
    });
    });

    // Benchmark charts
    let cacheChart, txThroughputChart, latencyDistChart, storageChart;

    function initBenchmarkCharts() {
    // Cache performance chart
    const cacheCtx = document.getElementById('cacheChart').getContext('2d');
    cacheChart = new Chart(cacheCtx, {
      type: 'doughnut',
      data: {
        labels: ['Cache Hits', 'Cache Misses'],
        datasets: [{
          data: [0, 0],
          backgroundColor: ['#4CAF50', '#F44336'],
          hoverBackgroundColor: ['#45a049', '#e53935']
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: 'bottom'
          }
        }
      }
    });

    // Transaction throughput chart
    const txCtx = document.getElementById('txThroughputChart').getContext('2d');
    txThroughputChart = new Chart(txCtx, {
      type: 'bar',
      data: {
        labels: ['Current'],
        datasets: [{
          label: 'Transactions/sec',
          data: [0],
          backgroundColor: '#f39c12'
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: 'Tx/sec'
            }
          }
        }
      }
    });

    // Latency distribution chart
    const latencyCtx = document.getElementById('latencyDistChart').getContext('2d');
    latencyDistChart = new Chart(latencyCtx, {
      type: 'line',
      data: {
        labels: ['min', 'p25', 'p50', 'p75', 'p95', 'max'],
        datasets: [{
          label: 'Latency (ms)',
          data: [0, 0, 0, 0, 0, 0],
          fill: true,
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
          borderColor: 'rgba(75, 192, 192, 1)',
          tension: 0.4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true
          }
        }
      }
    });

    // Storage chart (Bar chart showing nodes and edges)
    const storageCtx = document.getElementById('storageChart').getContext('2d');
    storageChart = new Chart(storageCtx, {
      type: 'bar',
      data: {
        labels: ['Nodes', 'Edges'],
        datasets: [{
          label: 'Count',
          data: [0, 0],
          backgroundColor: ['#3498db', '#9b59b6']
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true
          }
        }
      }
    });
    }

    function loadBenchmarkData() {
    fetch('/metrics')
      .then(response => response.json())
      .then(data => {
        if (data.status === 'success') {
          const metrics = data.data;
          console.log("Loading benchmark data:", metrics);

          // Update cache chart
          if (metrics.cache) {
            cacheChart.data.datasets[0].data = [
              metrics.cache.hits,
              metrics.cache.misses
            ];
            cacheChart.update();

            // Update cache metrics display
            document.getElementById('cacheHitRate').textContent = `${metrics.cache.hit_rate.toFixed(1)}%`;
            document.getElementById('cacheSize').textContent = `${metrics.cache.size} entries`;
          }

          // Update storage metrics display and chart
          if (metrics.storage) {
            document.getElementById('nodeCount').textContent = metrics.storage.node_count.toLocaleString();
            document.getElementById('edgeCount').textContent = metrics.storage.edge_count.toLocaleString();

            storageChart.data.datasets[0].data = [
              metrics.storage.node_count,
              metrics.storage.edge_count
            ];
            storageChart.update();
          }

          // Update benchmark data if available
          if (metrics.benchmarks && metrics.benchmarks.latest) {
            const latest = metrics.benchmarks.latest;
            console.log("Latest benchmark data:", latest);

            // Update transaction throughput chart
            txThroughputChart.data.labels = ['Latest Benchmark'];
            txThroughputChart.data.datasets[0].data = [latest.transactions_per_second || 0];
            txThroughputChart.update();

            // Update throughput metrics display
            document.getElementById('txPerSecond').textContent = `${(latest.transactions_per_second || 0).toFixed(2)}`;
            document.getElementById('totalTime').textContent = `${(latest.total_time_ms || 0) / 1000} sec`;

            // Update latency distribution chart if percentiles are available
            if (latest.percentiles) {
              console.log("Percentiles data:", latest.percentiles);
              const percentiles = latest.percentiles;

              // Calculate missing percentiles with fallbacks
              const minVal = percentiles.min !== undefined ? percentiles.min : 0;
              const maxVal = percentiles.max !== undefined ? percentiles.max : 0;
              const p50Val = percentiles.p50 || 0;
              const p95Val = percentiles.p95 || 0;

              // Use existing values or calculate estimates
              const p25Val = percentiles.p25 !== undefined ? percentiles.p25 : ((minVal + p50Val) / 2);
              const p75Val = percentiles.p75 !== undefined ? percentiles.p75 : ((p50Val + p95Val) / 2);

              console.log("Calculated percentiles for chart:", [minVal, p25Val, p50Val, p75Val, p95Val, maxVal]);

              // Convert to milliseconds for display
              latencyDistChart.data.datasets[0].data = [
                minVal / 1000,  // Convert microseconds to milliseconds
                p25Val / 1000,
                p50Val / 1000,
                p75Val / 1000,
                p95Val / 1000,
                maxVal / 1000
              ];

              latencyDistChart.update();

              // Update latency metrics display
              document.getElementById('avgLatency').textContent = `${(latest.average_tx_time_us / 1000 || 0).toFixed(2)} ms`;
              document.getElementById('p95Latency').textContent = `${(p95Val / 1000 || 0).toFixed(2)} ms`;
            } else if (latest.average_tx_time_us) {
              // Fallback if percentiles aren't available
              document.getElementById('avgLatency').textContent = `${(latest.average_tx_time_us / 1000 || 0).toFixed(2)} ms`;
              document.getElementById('p95Latency').textContent = `N/A`;
            }
          }

          // Update benchmark history table
          if (metrics.benchmarks && metrics.benchmarks.results) {
            const historyTable = document.getElementById('benchmarkHistory');
            historyTable.innerHTML = '';

            metrics.benchmarks.results.forEach(result => {
              const row = document.createElement('tr');
              row.innerHTML = `
                <td class="px-4 py-2">${result.timestamp || 'N/A'}</td>
                <td class="px-4 py-2">${result.total_transactions || 'N/A'}</td>
                <td class="px-4 py-2">${result.concurrent_connections || 'N/A'}</td>
                <td class="px-4 py-2">${(result.transactions_per_second || 0).toFixed(2)}</td>
                <td class="px-4 py-2">${(result.average_tx_time_us / 1000 || 0).toFixed(2)} ms</td>
                <td class="px-4 py-2">${result.percentiles?.p95 ?
                                      (result.percentiles.p95 / 1000).toFixed(2) : 'N/A'} ms</td>
              `;
              historyTable.appendChild(row);
            });

            if (metrics.benchmarks.results.length === 0) {
              const row = document.createElement('tr');
              row.innerHTML = `
                <td colspan="6" class="px-4 py-4 text-center text-gray-500">No benchmark data available. Run a benchmark to see results.</td>
              `;
              historyTable.appendChild(row);
            }
          }
        } else {
          console.error('Error loading metrics:', data.message);
        }
      })
      .catch(error => {
        console.error('Error fetching metrics:', error);
      });
    }

    // Function to load validator metrics
    function loadValidatorMetrics() {
    console.log("Loading validator metrics...");
    fetch('/validator-metrics')
      .then(response => response.json())
      .then(data => {
        if (data.status === 'success') {
          console.log("Validator metrics data:", data.data);
          const tableBody = document.getElementById('validatorMetricsBody');
          tableBody.innerHTML = '';

          Object.entries(data.data).forEach(([validator, metrics]) => {
            const row = document.createElement('tr');

            // Format timestamp
            const lastActive = metrics.last_active ? new Date(metrics.last_active).toLocaleString() : 'N/A';

            // Calculate success rate
            const successRate = metrics.total_transactions > 0
              ? ((metrics.successful_transactions / metrics.total_transactions) * 100).toFixed(1)
              : 'N/A';

            // Format avg tx time
            const avgTxTime = metrics.avg_tx_time
              ? `${metrics.avg_tx_time.toFixed(2)}μs`
              : 'N/A';

            row.innerHTML = `
              <td class="px-4 py-2 font-medium">${validator}</td>
              <td class="px-4 py-2">${metrics.total_transactions}</td>
              <td class="px-4 py-2">${successRate}%</td>
              <td class="px-4 py-2">${avgTxTime}</td>
              <td class="px-4 py-2">${lastActive}</td>
            `;

            tableBody.appendChild(row);
          });
        }
      })
      .catch(error => console.error('Error loading validator metrics:', error));

    // Get current validator
    fetch('/validator-status')
      .then(response => response.json())
      .then(data => {
        if (data.status === 'success') {
          console.log("Validator status:", data.data);
          document.getElementById('currentValidator').textContent =
            data.data.current_validator || 'None';
        }
      })
      .catch(error => console.error('Error loading validator status:', error));
    }

    // Load metrics initially and refresh every 5 seconds
    loadValidatorMetrics();
    setInterval(loadValidatorMetrics, 5000);
    </script>

    </body>
    </html>
    """
  end
end
