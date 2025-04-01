/**
 * Standardize phone number format
 * @param {String} phone - Phone number to standardize
 * @returns {String|null} - Standardized phone number or null
 */
function standardizePhoneNumber(phone) {
    if (!phone) return null;
    // Remove all non-numeric characters
    return phone.replace(/\D/g, '').slice(-10);
  }
  
  /**
   * Format date to MM/DD/YYYY
   * @param {String|Date} date - Date to format
   * @returns {String|null} - Formatted date or null
   */
  function formatDate(date) {
    if (!date) return null;
    
    try {
      // Ensure we're working with a date object
      const d = new Date(date);
      if (isNaN(d.getTime())) return null; // Invalid date
      
      // Format as MM/DD/YYYY
      return `${(d.getMonth() + 1).toString().padStart(2, '0')}/${d.getDate().toString().padStart(2, '0')}/${d.getFullYear()}`;
    } catch (error) {
      handleError(error, 'Error formatting date:');
      return null;
    }
  }
  
  /**
   * Format date from Airtable format to standard format
   * @param {String} airtableDate - Date from Airtable
   * @returns {String} - Standardized date
   */
  function formatAirtableDate(airtableDate) {
    if (!airtableDate) return '';
    
    try {
      // Handle different possible formats
      if (airtableDate.includes('-')) {
        // YYYY-MM-DD format
        const [year, month, day] = airtableDate.split('-');
        return `${month}/${day}/${year}`;
      } else if (airtableDate.includes('/')) {
        // Already in MM/DD/YYYY format
        return airtableDate;
      }
      
      // Try to parse as date
      return formatDate(airtableDate);
    } catch (error) {
      handleError(error, 'Error formatting Airtable date:');
      return airtableDate; // Return as-is if format is unexpected
    }
  }
  
  /**
   * Get tenant state from tenant ID
   * @param {String|Number} tenantId - Tenant ID
   * @returns {String} - State code
   */
  function getTenantState(tenantId) {
    const stateMap = {
      '2': 'CA',
      '3': 'LA',
      '4': 'GA',
      '5': 'NM',
      '6': 'CA',
      '13': 'UK'
    };
    return stateMap[tenantId.toString()] || 'Unknown';
  }
  
  module.exports = {
    standardizePhoneNumber,
    formatDate,
    formatAirtableDate,
    getTenantState
  };