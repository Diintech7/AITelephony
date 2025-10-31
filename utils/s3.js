const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3')
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner')
const path = require('path')

// Lazily create S3 client to avoid startup errors if envs are missing
let s3Client = null

const getS3Client = () => {
  if (s3Client) return s3Client
  const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION
  s3Client = new S3Client({ region })
  return s3Client
}

/**
 * Upload a buffer to S3
 * @param {Buffer} buffer - File contents
 * @param {string} bucket - Bucket name
 * @param {string} key - Object key (path in bucket)
 * @param {string} contentType - MIME type (e.g., audio/wav)
 * @returns {string} publicUrl (if bucket is public) or s3:// url
 */
const uploadBufferToS3 = async (buffer, bucket, key, contentType = 'audio/wav') => {
  const client = getS3Client()
  const isPublic = String(process.env.AWS_S3_PUBLIC_READ || '').toLowerCase() === 'true'
  const cmd = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: buffer,
    ContentType: contentType,
    ...(isPublic ? { ACL: 'public-read' } : {}),
  })
  await client.send(cmd)
  const wantSigned = String(process.env.AWS_S3_RETURN_SIGNED_URL || '').toLowerCase() === 'true'
  if (wantSigned || !isPublic) {
    const expires = Number(process.env.AWS_S3_SIGNED_URL_EXPIRES || 3600)
    const signed = await getSignedUrl(client, new GetObjectCommand({ Bucket: bucket, Key: key }), { expiresIn: expires })
    return signed
  }
  const base = process.env.AWS_S3_PUBLIC_BASE_URL
  if (base) {
    const sep = base.endsWith('/') ? '' : '/'
    return `${base}${sep}${key}`
  }
  return `https://${bucket}.s3.${process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION}.amazonaws.com/${key}`
}
/**
 * Build an S3 key using optional prefix and the recording filename
 */
const buildRecordingKey = (recordingPath) => {
  const prefix = (process.env.AWS_S3_PREFIX || 'call-recordings').replace(/^\/+|\/+$/g, '')
  const file = path.posix.basename(String(recordingPath || 'recording.wav'))
  const y = new Date()
  const yyyy = y.getUTCFullYear()
  const mm = String(y.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(y.getUTCDate()).padStart(2, '0')
  return `${prefix}/${yyyy}/${mm}/${dd}/${file}`
}

module.exports = {
  uploadBufferToS3,
  buildRecordingKey,
}


