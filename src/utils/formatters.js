function standardizePhoneNumber(phone) {
    if (!phone) return null;
    // Remove all non-numeric characters
    return phone.replace(/\D/g, '').slice(-10);
  }
  
  function formatDate(date) {
    if (!date) return null;
    const [year, month, day] = date.split('-');
    return `${month}/${day}/${year}`;
  }
  
  module.exports = {
    standardizePhoneNumber,
    formatDate
  };