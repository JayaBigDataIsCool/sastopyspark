import React, { useState } from 'react';
import axios from 'axios';
import './SASConverter.css';

// Replace with your actual API Gateway endpoint
const API_ENDPOINT = 'https://YOUR-API-GATEWAY-ID.execute-api.REGION.amazonaws.com/prod/convert';

function SASConverter() {
  const [sasCode, setSasCode] = useState('');
  const [pysparkCode, setPysparkCode] = useState('');
  const [annotations, setAnnotations] = useState([]);
  const [warnings, setWarnings] = useState([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState(null);
  
  // Form options
  const [options, setOptions] = useState({
    add_detailed_comments: true,
    apply_immediate_optimizations: true,
    extreme_mode: false,
    disable_splitting: false,
    strategy_override: ''
  });

  const handleOptionChange = (e) => {
    const { name, type, checked, value } = e.target;
    setOptions({
      ...options,
      [name]: type === 'checkbox' ? checked : value
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!sasCode.trim()) {
      setError('Please enter SAS code to convert');
      return;
    }
    
    setLoading(true);
    setError('');
    setPysparkCode('');
    setAnnotations([]);
    setWarnings([]);
    setStats(null);
    
    try {
      // Filter out empty strategy override
      const requestOptions = { ...options };
      if (!requestOptions.strategy_override) {
        delete requestOptions.strategy_override;
      }
      
      const response = await axios.post(API_ENDPOINT, {
        sas_code: sasCode,
        options: requestOptions
      });
      
      const data = response.data;
      
      if (data.status === 'error') {
        throw new Error(data.message || 'Conversion failed');
      }
      
      setPysparkCode(data.pyspark_code);
      setAnnotations(data.annotations || []);
      setWarnings(data.warnings || []);
      setStats(data.processing_stats);
      
      if (!data.processing_complete) {
        setWarnings(prev => [...prev, 'Processing was incomplete due to time limitations. Results may be partial.']);
      }
    } catch (err) {
      console.error('Conversion error:', err);
      setError(err.response?.data?.message || err.message || 'Failed to convert SAS code');
    } finally {
      setLoading(false);
    }
  };

  const copyToClipboard = () => {
    navigator.clipboard.writeText(pysparkCode);
    // You could add a toast notification here
  };

  return (
    <div className="sas-converter">
      <h1>SAS to PySpark Converter</h1>
      <div className="converter-container">
        <form onSubmit={handleSubmit}>
          <div className="form-group">
            <label htmlFor="sasCode">SAS Code:</label>
            <textarea
              id="sasCode"
              value={sasCode}
              onChange={(e) => setSasCode(e.target.value)}
              placeholder="Paste your SAS code here..."
              className="code-area"
              rows={15}
            />
          </div>
          
          <div className="options-container">
            <h3>Conversion Options</h3>
            <div className="options-grid">
              <div className="option">
                <input
                  type="checkbox"
                  id="add_detailed_comments"
                  name="add_detailed_comments"
                  checked={options.add_detailed_comments}
                  onChange={handleOptionChange}
                />
                <label htmlFor="add_detailed_comments">Add detailed comments</label>
              </div>
              
              <div className="option">
                <input
                  type="checkbox"
                  id="apply_immediate_optimizations"
                  name="apply_immediate_optimizations"
                  checked={options.apply_immediate_optimizations}
                  onChange={handleOptionChange}
                />
                <label htmlFor="apply_immediate_optimizations">Apply immediate optimizations</label>
              </div>
              
              <div className="option">
                <input
                  type="checkbox"
                  id="extreme_mode"
                  name="extreme_mode"
                  checked={options.extreme_mode}
                  onChange={handleOptionChange}
                />
                <label htmlFor="extreme_mode">Extreme mode (for very large files)</label>
              </div>
              
              <div className="option">
                <input
                  type="checkbox"
                  id="disable_splitting"
                  name="disable_splitting"
                  checked={options.disable_splitting}
                  onChange={handleOptionChange}
                />
                <label htmlFor="disable_splitting">Disable code splitting</label>
              </div>
            </div>
            
            <div className="form-group">
              <label htmlFor="strategy_override">Conversion Strategy:</label>
              <select
                id="strategy_override"
                name="strategy_override"
                value={options.strategy_override}
                onChange={handleOptionChange}
                className="strategy-select"
              >
                <option value="">Auto-detect (recommended)</option>
                <option value="Simple Direct">Simple Direct</option>
                <option value="Standard Multi-Pass">Standard Multi-Pass</option>
                <option value="Deep Macro Analysis">Deep Macro Analysis</option>
              </select>
            </div>
          </div>
          
          <button 
            type="submit" 
            className="convert-button" 
            disabled={loading}
          >
            {loading ? 'Converting...' : 'Convert to PySpark'}
          </button>
        </form>
        
        {loading && (
          <div className="loader-container">
            <div className="loader"></div>
            <p>Converting SAS to PySpark... This may take a minute for large code blocks.</p>
          </div>
        )}
        
        {error && (
          <div className="error-message">
            <h3>Error</h3>
            <p>{error}</p>
          </div>
        )}
        
        {pysparkCode && (
          <div className="result-container">
            <div className="result-header">
              <h2>PySpark Code</h2>
              <button onClick={copyToClipboard} className="copy-button">
                Copy to Clipboard
              </button>
            </div>
            <pre className="code-output">{pysparkCode}</pre>
          </div>
        )}
        
        {warnings.length > 0 && (
          <div className="warnings-container">
            <h3>Warnings</h3>
            <ul>
              {warnings.map((warning, i) => (
                <li key={i} className="warning-item">{warning}</li>
              ))}
            </ul>
          </div>
        )}
        
        {annotations.length > 0 && (
          <div className="annotations-container">
            <h3>Annotations</h3>
            {annotations.map((annotation, i) => (
              <div key={i} className="annotation-item">
                <div className="annotation-header">
                  <span className="annotation-lines">
                    SAS Lines: {annotation.sas_lines.join('-')}
                  </span>
                  {annotation.severity && (
                    <span className={`annotation-severity ${annotation.severity.toLowerCase()}`}>
                      {annotation.severity}
                    </span>
                  )}
                </div>
                <p className="annotation-note">{annotation.note}</p>
              </div>
            ))}
          </div>
        )}
        
        {stats && (
          <div className="stats-container">
            <h3>Processing Statistics</h3>
            <table className="stats-table">
              <tbody>
                <tr>
                  <td>Strategy Used:</td>
                  <td>{stats.strategy_used || 'Standard'}</td>
                </tr>
                <tr>
                  <td>Processing Time:</td>
                  <td>{stats.total_duration_seconds.toFixed(2)} seconds</td>
                </tr>
                <tr>
                  <td>Chunks Processed:</td>
                  <td>{stats.chunks_processed}</td>
                </tr>
                <tr>
                  <td>API Calls:</td>
                  <td>{stats.llm_calls}</td>
                </tr>
                {stats.token_usage && (
                  <>
                    <tr>
                      <td>Total Tokens:</td>
                      <td>{stats.token_usage.total_tokens}</td>
                    </tr>
                    <tr>
                      <td>Input Tokens:</td>
                      <td>{stats.token_usage.input_tokens}</td>
                    </tr>
                    <tr>
                      <td>Output Tokens:</td>
                      <td>{stats.token_usage.output_tokens}</td>
                    </tr>
                  </>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

export default SASConverter; 