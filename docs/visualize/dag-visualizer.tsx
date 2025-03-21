import React, { useState, useEffect, useRef } from 'react';
import * as d3 from 'd3';

const KylixDAGVisualizer = () => {
  const svgRef = useRef();
  const [data, setData] = useState(null);
  const [filteredData, setFilteredData] = useState(null);
  const [filters, setFilters] = useState({
    subject: '',
    predicate: '',
    object: ''
  });
  const [filterOptions, setFilterOptions] = useState({
    subjects: [],
    predicates: [],
    objects: []
  });
  const [selectedNode, setSelectedNode] = useState(null);

  // Load the mock transaction data
  useEffect(() => {
    const mockTransactions = [
      {
        id: "tx1",
        data: {
          subject: "Alice",
          predicate: "knows",
          object: "Bob",
          validator: "agent1",
          timestamp: new Date().toISOString()
        },
        edges: [
          { to: "tx2", label: "confirms" }
        ]
      },
      {
        id: "tx2",
        data: {
          subject: "Bob",
          predicate: "knows",
          object: "Charlie",
          validator: "agent2",
          timestamp: new Date().toISOString()
        },
        edges: [
          { to: "tx3", label: "confirms" }
        ]
      },
      {
        id: "tx3",
        data: {
          subject: "Charlie",
          predicate: "knows",
          object: "Dave",
          validator: "agent1",
          timestamp: new Date().toISOString()
        },
        edges: []
      },
      {
        id: "tx4",
        data: {
          subject: "Alice",
          predicate: "likes",
          object: "Coffee",
          validator: "agent2",
          timestamp: new Date().toISOString()
        },
        edges: []
      },
      {
        id: "tx5",
        data: {
          subject: "Bob",
          predicate: "likes",
          object: "Tea",
          validator: "agent1",
          timestamp: new Date().toISOString()
        },
        edges: []
      }
    ];

    // Add additional edges for the social graph structure
    mockTransactions[0].edges.push({ to: "tx4", label: "same_subject" }); // Alice knows Bob -> Alice likes Coffee
    mockTransactions[1].edges.push({ to: "tx5", label: "same_subject" }); // Bob knows Charlie -> Bob likes Tea

    // Transform data for visualization
    const graphData = transformToGraphData(mockTransactions);
    setData(graphData);
    setFilteredData(graphData);

    // Extract filter options
    setFilterOptions({
      subjects: [...new Set(mockTransactions.map(tx => tx.data.subject))],
      predicates: [...new Set(mockTransactions.map(tx => tx.data.predicate))],
      objects: [...new Set(mockTransactions.map(tx => tx.data.object))]
    });
  }, []);

  // Transform transactions to graph data
  const transformToGraphData = (transactions) => {
    const nodes = [];
    const links = [];
    
    // Create nodes
    transactions.forEach(tx => {
      nodes.push({
        id: tx.id,
        subject: tx.data.subject,
        predicate: tx.data.predicate,
        object: tx.data.object,
        validator: tx.data.validator,
        timestamp: tx.data.timestamp,
        // Categorize nodes by subject to assign colors
        group: tx.data.subject
      });
      
      // Create links (edges)
      tx.edges.forEach(edge => {
        links.push({
          source: tx.id,
          target: edge.to,
          label: edge.label
        });
      });
    });
    
    return { nodes, links };
  };

  // Handle filter changes
  const handleFilterChange = (filterType, value) => {
    const newFilters = { ...filters, [filterType]: value };
    setFilters(newFilters);
    
    if (data) {
      // Apply filters to nodes
      const filteredNodes = data.nodes.filter(node => {
        return (!newFilters.subject || node.subject === newFilters.subject) &&
               (!newFilters.predicate || node.predicate === newFilters.predicate) &&
               (!newFilters.object || node.object === newFilters.object);
      });
      
      // Get IDs of filtered nodes
      const filteredNodeIds = filteredNodes.map(node => node.id);
      
      // Include only links where both source and target are in the filtered nodes
      const filteredLinks = data.links.filter(link => {
        const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
        const targetId = typeof link.target === 'object' ? link.target.id : link.target;
        return filteredNodeIds.includes(sourceId) && filteredNodeIds.includes(targetId);
      });
      
      setFilteredData({ nodes: filteredNodes, links: filteredLinks });
    }
  };

  // Reset all filters
  const resetFilters = () => {
    setFilters({ subject: '', predicate: '', object: '' });
    setFilteredData(data);
  };

  // Render the graph visualization using D3
  useEffect(() => {
    if (!filteredData || !svgRef.current) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove(); // Clear previous visualization
    
    const width = 800;
    const height = 600;
    
    // Create the SVG container
    const container = svg
      .attr("width", width)
      .attr("height", height)
      .append("g")
      .attr("transform", "translate(0, 0)");
    
    // Define arrow markers for links
    svg.append("defs").selectAll("marker")
      .data(["arrow"])
      .enter().append("marker")
      .attr("id", d => d)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 25)
      .attr("refY", 0)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("path")
      .attr("fill", "#999")
      .attr("d", "M0,-5L10,0L0,5");
    
    // Create a force simulation
    const simulation = d3.forceSimulation(filteredData.nodes)
      .force("link", d3.forceLink(filteredData.links).id(d => d.id).distance(150))
      .force("charge", d3.forceManyBody().strength(-300))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide().radius(50));
    
    // Create a color scale based on groups (subjects)
    const color = d3.scaleOrdinal(d3.schemeCategory10);
    
    // Create links
    const link = container.append("g")
      .attr("class", "links")
      .selectAll("g")
      .data(filteredData.links)
      .enter().append("g");
    
    // Draw the link lines
    link.append("line")
      .attr("stroke", "#999")
      .attr("stroke-opacity", 0.6)
      .attr("stroke-width", 1.5)
      .attr("marker-end", "url(#arrow)");
    
    // Add link labels
    link.append("text")
      .attr("font-size", 10)
      .attr("fill", "#666")
      .attr("text-anchor", "middle")
      .text(d => d.label);
    
    // Create node groups
    const node = container.append("g")
      .attr("class", "nodes")
      .selectAll("g")
      .data(filteredData.nodes)
      .enter().append("g")
      .call(d3.drag()
        .on("start", dragStarted)
        .on("drag", dragged)
        .on("end", dragEnded))
      .on("click", (event, d) => {
        setSelectedNode(d);
      });
    
    // Add node circles
    node.append("circle")
      .attr("r", 20)
      .attr("fill", d => color(d.group))
      .attr("stroke", "#fff")
      .attr("stroke-width", 1.5);
    
    // Add node labels
    node.append("text")
      .attr("font-size", 10)
      .attr("text-anchor", "middle")
      .attr("dy", 5)
      .text(d => d.id);
    
    // Add node tooltips
    node.append("title")
      .text(d => `${d.subject} ${d.predicate} ${d.object}`);
    
    // Update positions during simulation
    simulation.on("tick", () => {
      link.select("line")
        .attr("x1", d => d.source.x)
        .attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x)
        .attr("y2", d => d.target.y);
      
      link.select("text")
        .attr("x", d => (d.source.x + d.target.x) / 2)
        .attr("y", d => (d.source.y + d.target.y) / 2);
      
      node.attr("transform", d => `translate(${d.x},${d.y})`);
    });
    
    // Define drag functions
    function dragStarted(event, d) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      d.fx = d.x;
      d.fy = d.y;
    }
    
    function dragged(event, d) {
      d.fx = event.x;
      d.fy = event.y;
    }
    
    function dragEnded(event, d) {
      if (!event.active) simulation.alphaTarget(0);
      d.fx = null;
      d.fy = null;
    }
    
    // Cleanup function
    return () => {
      simulation.stop();
    };
  }, [filteredData]);

  return (
    <div className="flex flex-col h-full">
      <div className="bg-gray-100 p-4 rounded-lg shadow mb-4">
        <h1 className="text-2xl font-bold mb-2">Kylix Blockchain DAG Visualizer</h1>
        <p className="text-gray-700 mb-4">
          Visualize the transaction graph of the Kylix blockchain. Filter by subject, predicate, or object to explore relationships.
        </p>
        
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Subject</label>
            <select 
              className="w-full p-2 border border-gray-300 rounded"
              value={filters.subject}
              onChange={(e) => handleFilterChange('subject', e.target.value)}
            >
              <option value="">All</option>
              {filterOptions.subjects.map(subject => (
                <option key={subject} value={subject}>{subject}</option>
              ))}
            </select>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Predicate</label>
            <select 
              className="w-full p-2 border border-gray-300 rounded"
              value={filters.predicate}
              onChange={(e) => handleFilterChange('predicate', e.target.value)}
            >
              <option value="">All</option>
              {filterOptions.predicates.map(predicate => (
                <option key={predicate} value={predicate}>{predicate}</option>
              ))}
            </select>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Object</label>
            <select 
              className="w-full p-2 border border-gray-300 rounded"
              value={filters.object}
              onChange={(e) => handleFilterChange('object', e.target.value)}
            >
              <option value="">All</option>
              {filterOptions.objects.map(object => (
                <option key={object} value={object}>{object}</option>
              ))}
            </select>
          </div>
          
          <div className="flex items-end">
            <button 
              className="bg-blue-500 hover:bg-blue-600 text-white py-2 px-4 rounded"
              onClick={resetFilters}
            >
              Reset Filters
            </button>
          </div>
        </div>
      </div>
      
      <div className="flex flex-grow">
        <div className="flex-grow bg-white rounded-lg shadow overflow-hidden">
          <svg ref={svgRef} className="w-full h-full"></svg>
        </div>
        
        {selectedNode && (
          <div className="w-64 ml-4 bg-white p-4 rounded-lg shadow">
            <h2 className="text-lg font-semibold mb-2">Transaction Details</h2>
            <div className="text-sm">
              <p><span className="font-medium">ID:</span> {selectedNode.id}</p>
              <p><span className="font-medium">Subject:</span> {selectedNode.subject}</p>
              <p><span className="font-medium">Predicate:</span> {selectedNode.predicate}</p>
              <p><span className="font-medium">Object:</span> {selectedNode.object}</p>
              <p><span className="font-medium">Validator:</span> {selectedNode.validator}</p>
              <p><span className="font-medium">Timestamp:</span> {new Date(selectedNode.timestamp).toLocaleString()}</p>
            </div>
            <button 
              className="mt-4 bg-gray-200 hover:bg-gray-300 text-gray-800 py-1 px-3 rounded text-sm"
              onClick={() => setSelectedNode(null)}
            >
              Close
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default KylixDAGVisualizer;
