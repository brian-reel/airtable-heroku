/**
 * Validate email format
 * @param {String} email - Email to validate
 * @returns {Boolean} - Whether the email is valid
 */
function isValidEmail(email) {
  if (!email) return false;
  
  // Basic email validation regex
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

/**
 * Validate phone number format
 * @param {String} phone - Phone number to validate
 * @returns {Boolean} - Whether the phone number is valid
 */
function isValidPhone(phone) {
  if (!phone) return false;
  
  // Remove all non-numeric characters and check length
  const digitsOnly = phone.replace(/\D/g, '');
  return digitsOnly.length >= 10;
}

/**
 * Validate date format
 * @param {String|Date} date - Date to validate
 * @returns {Boolean} - Whether the date is valid
 */
function isValidDate(date) {
  if (!date) return false;
  
  try {
    const d = new Date(date);
    return !isNaN(d.getTime());
  } catch (error) {
    return false;
  }
}

/**
 * Validate employee data
 * @param {Object} employee - Employee data to validate
 * @returns {Object} - Validation result with errors
 */
function validateEmployee(employee) {
  const errors = [];
  
  // Required fields
  if (!employee.name) {
    errors.push('Name is required');
  }
  
  // Validate email if present
  if (employee.email && !isValidEmail(employee.email)) {
    errors.push('Invalid email format');
  }
  
  // Validate phone if present
  if (employee.mobile_phone && !isValidPhone(employee.mobile_phone)) {
    errors.push('Invalid phone number format');
  }
  
  // Validate hire date if present
  if (employee.hire_date && !isValidDate(employee.hire_date)) {
    errors.push('Invalid hire date');
  }
  
  return {
    isValid: errors.length === 0,
    errors
  };
}

/**
 * Validate Airtable record
 * @param {Object} record - Airtable record to validate
 * @returns {Object} - Validation result with errors
 */
function validateAirtableRecord(record) {
  const errors = [];
  const fields = record.fields || {};
  
  // Validate email if present
  if (fields['Email'] && !isValidEmail(fields['Email'])) {
    errors.push('Invalid email format');
  }
  
  // Validate phone if present
  if (fields['Phone'] && !isValidPhone(fields['Phone'])) {
    errors.push('Invalid phone number format');
  }
  
  // Validate hire date if present
  if (fields['RSC Hire Date'] && !isValidDate(fields['RSC Hire Date'])) {
    errors.push('Invalid hire date');
  }
  
  return {
    isValid: errors.length === 0,
    errors
  };
}

/**
 * Validate update fields
 * @param {Object} fields - Fields to update
 * @returns {Object} - Validation result with errors
 */
function validateUpdateFields(fields) {
  const errors = [];
  
  // Validate email if present
  if (fields['Email'] && !isValidEmail(fields['Email'])) {
    errors.push('Invalid email format');
  }
  
  // Validate phone if present
  if (fields['Phone'] && !isValidPhone(fields['Phone'])) {
    errors.push('Invalid phone number format');
  }
  
  // Validate hire date if present
  if (fields['RSC Hire Date'] && !isValidDate(fields['RSC Hire Date'])) {
    errors.push('Invalid hire date');
  }
  
  // Validate status fields
  if (fields['Status-RSPG'] && !['Active', 'Inactive'].includes(fields['Status-RSPG'])) {
    errors.push('Invalid Status-RSPG value. Must be "Active" or "Inactive"');
  }
  
  if (fields['Status'] && !['Hired', 'Separated'].includes(fields['Status'])) {
    errors.push('Invalid Status value. Must be "Hired" or "Separated"');
  }
  
  return {
    isValid: errors.length === 0,
    errors
  };
}

module.exports = {
  isValidEmail,
  isValidPhone,
  isValidDate,
  validateEmployee,
  validateAirtableRecord,
  validateUpdateFields
}; 