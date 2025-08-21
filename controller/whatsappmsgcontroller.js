const axios = require('axios');
require('dotenv').config();

// WhatsApp Cloud API configuration
const WHATSAPP_TOKEN = process.env.WHATSAPP_TOKEN;
const WHATSAPP_PHONE_ID = process.env.WHATSAPP_PHONE_ID;
const WHATSAPP_BUSINESS_ACCOUNT_ID = process.env.WHATSAPP_BUSINESS_ACCOUNT_ID;

// Normalize Indian numbers to E.164 (+91XXXXXXXXXX)
const normalizeToE164India = (phoneNumber) => {
    const digits = String(phoneNumber || "").replace(/\D+/g, "");
    if (!digits) {
        throw new Error('Invalid phone number');
    }
    // Always take last 10 as local mobile and prefix +91
    const last10 = digits.slice(-10);
    if (last10.length !== 10) {
        throw new Error('Invalid Indian mobile number');
    }
    return `+91${last10}`;
};

// Core utility to send WhatsApp message (reusable)
const sendWhatsApp = async (phoneNumber, message) => {
    if (!phoneNumber || !message) {
        throw new Error('Phone number and message are required');
    }

    const cleanPhoneNumber = normalizeToE164India(phoneNumber);

    const url = `https://graph.facebook.com/v18.0/${WHATSAPP_PHONE_ID}/messages`;
    const headers = {
        'Authorization': `Bearer ${WHATSAPP_TOKEN}`,
        'Content-Type': 'application/json'
    };
    const requestBody = {
        messaging_product: 'whatsapp',
        to: cleanPhoneNumber,
        type: 'text',
        text: { body: message }
    };

    const response = await axios.post(url, requestBody, { headers });
    return {
        messageId: response.data.messages?.[0]?.id,
        phoneNumber: cleanPhoneNumber,
        message,
        timestamp: new Date().toISOString()
    };
};

// Express handler that wraps the core utility
const sendWhatsAppMessage = async (req, res) => {
    try {
        const { phoneNumber, message } = req.body;
        const result = await sendWhatsApp(phoneNumber, message);
        res.status(200).json({ success: true, message: 'Message sent successfully', data: result });
    } catch (error) {
        console.error('WhatsApp API Error:', error.response?.data || error.message);
        if (error.response?.data?.error) {
            const whatsappError = error.response.data.error;
            return res.status(400).json({
                success: false,
                message: 'WhatsApp API Error',
                error: {
                    code: whatsappError.code,
                    message: whatsappError.message,
                    details: whatsappError.error_subcode || 'No additional details'
                }
            });
        }
        res.status(500).json({ success: false, message: 'Failed to send WhatsApp message', error: error.message });
    }
};

// Function to get WhatsApp message status (optional)
const getMessageStatus = async (req, res) => {
    try {
        const { messageId } = req.params;

        if (!messageId) {
            return res.status(400).json({
                success: false,
                message: 'Message ID is required'
            });
        }

        // ✅ Corrected URL
        const url = `https://graph.facebook.com/v18.0/${messageId}`;

        const headers = {
            'Authorization': `Bearer ${WHATSAPP_TOKEN}`
        };

        const response = await axios.get(url, { headers });

        res.status(200).json({
            success: true,
            data: response.data
        });

    } catch (error) {
        console.error('Error getting message status:', error);
        res.status(500).json({
            success: false,
            message: 'Failed to get message status',
            error: error.message
        });
    }
};

module.exports = {
    sendWhatsAppMessage,
    getMessageStatus,
    sendWhatsApp,
    normalizeToE164India
};