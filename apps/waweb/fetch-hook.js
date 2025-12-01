const axios = require('axios');
module.exports = async function fetchDocumentFromUrl(url, maxBytes) {
  try {
    const head = await axios.head(url, {
      timeout: 10000,
      validateStatus: (status) => status >= 200 && status < 400,
    });
    const lenHeader = head.headers && (head.headers['content-length'] || head.headers['Content-Length']);
    if (lenHeader) {
      const size = Number(lenHeader);
      if (Number.isFinite(size) && size > 0) {
        if (size <= maxBytes) {
          return { buffer: null, size, mime: head.headers['content-type'] || head.headers['Content-Type'] || null, disposition: null };
        }
        throw new Error('document_too_large');
      }
    }
  } catch (err) {
    // ignore head errors; fallback to GET
  }
  const response = await axios.get(url, {
    responseType: 'arraybuffer',
    maxContentLength: maxBytes,
    maxBodyLength: maxBytes,
    timeout: 20000,
    validateStatus: (status) => status >= 200 && status < 300,
  });
  const buffer = Buffer.from(response.data);
  if (buffer.length > maxBytes) {
    const error = new Error('document_too_large');
    error.disposition = response.headers && (response.headers['content-disposition'] || response.headers['Content-Disposition']);
    error.mime = response.headers && (response.headers['content-type'] || response.headers['Content-Type']) || null;
    throw error;
  }
  return {
    buffer,
    size: buffer.length,
    mime: response.headers && (response.headers['content-type'] || response.headers['Content-Type']) || null,
    disposition: response.headers && (response.headers['content-disposition'] || response.headers['Content-Disposition']) || null,
  };
};
