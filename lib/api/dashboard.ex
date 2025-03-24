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
    </head>
    <body class="bg-gray-100">
      <div class="container mx-auto px-4 py-8">
        <header class="mb-8">
          <h1 class="text-3xl font-bold text-gray-800">Kylix Blockchain Explorer</h1>
          <p class="text-gray-600">A DAG-based blockchain for provenance tracking</p>
        </header>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
          <!-- Transaction Explorer Section -->
          <div class="bg-white p-6 rounded-lg shadow-md">
            <h2 class="text-xl font-semibold mb-4">Transaction Explorer</h2>
            <div class="mb-4">
              <input id="txSearch" type="text" placeholder="Search by ID, subject, predicate, or object"
                class="w-full p-2 border border-gray-300 rounded">
            </div>
            <div class="mb-4">
              <button id="loadTxBtn" class="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600">
                Load Transactions
              </button>
            </div>
            <div id="txResults" class="mt-4 overflow-x-auto">
              <table class="min-w-full divide-y divide-gray-200">
                <thead>
                  <tr>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Subject</th>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Predicate</th>
                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Object</th>
                  </tr>
                </thead>
                <tbody id="txTableBody" class="divide-y divide-gray-200">
                  <!-- Transaction rows will go here -->
                </tbody>
              </table>
            </div>
          </div>

          <!-- SPARQL Query Section -->
          <div class="bg-white p-6 rounded-lg shadow-md">
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
        </div>

        <!-- Transaction Submission Form -->
        <div class="mt-8 bg-white p-6 rounded-lg shadow-md">
          <h2 class="text-xl font-semibold mb-4">Submit Transaction</h2>
          <form id="txForm" class="grid grid-cols-1 md:grid-cols-2 gap-4">
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
            <div class="md:col-span-2">
              <button type="submit" class="bg-purple-500 text-white px-4 py-2 rounded hover:bg-purple-600">
                Submit Transaction
              </button>
            </div>
          </form>
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
            fetch('/transactions')
              .then(response => response.json())
              .then(data => {
                if (data.status === 'success') {
                  const tableBody = document.getElementById('txTableBody');
                  tableBody.innerHTML = '';

                  data.data.forEach(tx => {
                    const row = document.createElement('tr');
                    row.innerHTML = `
                      <td class="px-4 py-2">${tx.id}</td>
                      <td class="px-4 py-2">${tx.subject}</td>
                      <td class="px-4 py-2">${tx.predicate}</td>
                      <td class="px-4 py-2">${tx.object}</td>
                    `;
                    tableBody.appendChild(row);
                  });
                } else {
                  alert('Error loading transactions: ' + data.message);
                }
              })
              .catch(error => console.error('Error loading transactions:', error));
          });

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
                } else {
                  alert('Error submitting transaction: ' + data.message);
                }
              })
              .catch(error => console.error('Error submitting transaction:', error));
          });
        });
      </script>
    </body>
    </html>
    """
  end
end
