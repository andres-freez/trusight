require('dotenv').config();

console.log('HUBSPOT_PERSONAL_ACCESS_KEY:', process.env.HUBSPOT_PERSONAL_ACCESS_KEY ? '[SET]' : '[MISSING]');
console.log('Raw value (first 10 chars):', process.env.HUBSPOT_PERSONAL_ACCESS_KEY ? process.env.HUBSPOT_PERSONAL_ACCESS_KEY.slice(0, 10) + '...' : 'null');

