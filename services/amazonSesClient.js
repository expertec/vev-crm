function cleanString(value = '', maxLength = 260) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function toStringArray(values) {
  if (!values) return [];
  if (Array.isArray(values)) {
    return values
      .map((item) => cleanString(item, 320))
      .filter(Boolean);
  }
  return String(values)
    .split(',')
    .map((item) => cleanString(item, 320))
    .filter(Boolean);
}

function extractDkimTokens(value) {
  const rawTokens = Array.isArray(value)
    ? value
    : Array.isArray(value?.Tokens)
      ? value.Tokens
      : Array.isArray(value?.tokens)
        ? value.tokens
        : [];

  return rawTokens
    .map((item) => cleanString(item, 260).toLowerCase())
    .filter(Boolean);
}

let cachedSesModulePromise = null;

async function loadSesModule() {
  if (!cachedSesModulePromise) {
    cachedSesModulePromise = import('@aws-sdk/client-sesv2');
  }
  return cachedSesModulePromise;
}

export class AmazonSesClientError extends Error {
  constructor(
    message,
    {
      code = 'SES_CLIENT_ERROR',
      statusCode = 502,
      details = null,
    } = {}
  ) {
    super(message);
    this.name = 'AmazonSesClientError';
    this.code = code;
    this.statusCode = statusCode;
    this.details = details;
  }
}

function mapSesError(error) {
  if (error instanceof AmazonSesClientError) return error;

  const name = cleanString(error?.name || '', 120);
  const message = cleanString(error?.message || 'Error al conectar con Amazon SES', 360);

  if (name === 'NotFoundException') {
    return new AmazonSesClientError('Identidad SES no encontrada', {
      code: 'SES_IDENTITY_NOT_FOUND',
      statusCode: 404,
      details: { cause: name, message },
    });
  }

  if (name === 'TooManyRequestsException' || name === 'LimitExceededException') {
    return new AmazonSesClientError(message || 'Límite de Amazon SES excedido', {
      code: 'SES_RATE_LIMITED',
      statusCode: 429,
      details: { cause: name },
    });
  }

  if (name === 'AccessDeniedException' || name === 'UnauthorizedException') {
    return new AmazonSesClientError(message || 'Acceso denegado a Amazon SES', {
      code: 'SES_ACCESS_DENIED',
      statusCode: 403,
      details: { cause: name },
    });
  }

  if (name === 'BadRequestException') {
    return new AmazonSesClientError(message || 'Solicitud inválida para Amazon SES', {
      code: 'SES_BAD_REQUEST',
      statusCode: 400,
      details: { cause: name },
    });
  }

  if (name === 'AlreadyExistsException') {
    return new AmazonSesClientError(message || 'La identidad SES ya existe', {
      code: 'SES_IDENTITY_ALREADY_EXISTS',
      statusCode: 409,
      details: { cause: name },
    });
  }

  if (name === 'SignatureDoesNotMatch' || name === 'InvalidSignatureException') {
    return new AmazonSesClientError(message || 'Credenciales AWS inválidas para SES', {
      code: 'SES_INVALID_SIGNATURE',
      statusCode: 401,
      details: { cause: name },
    });
  }

  return new AmazonSesClientError(message || 'Error al conectar con Amazon SES', {
    code: cleanString(error?.code || 'SES_API_ERROR', 120),
    statusCode: Number.isInteger(error?.statusCode) ? error.statusCode : 502,
    details: { cause: name || undefined },
  });
}

export class AmazonSesClient {
  constructor({
    region = process.env.AWS_REGION
      || process.env.AWS_DEFAULT_REGION
      || process.env.AMAZON_SES_REGION
      || '',
    accessKeyId = process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || '',
    sessionToken = process.env.AWS_SESSION_TOKEN || '',
    logger = console,
  } = {}) {
    this.region = cleanString(region, 80);
    this.accessKeyId = cleanString(accessKeyId, 220);
    this.secretAccessKey = cleanString(secretAccessKey, 320);
    this.sessionToken = cleanString(sessionToken, 400);
    this.logger = logger;
    this.client = null;
  }

  assertReady() {
    if (!this.region) {
      throw new AmazonSesClientError('Falta AWS_REGION para usar Amazon SES', {
        code: 'SES_REGION_REQUIRED',
        statusCode: 503,
      });
    }
  }

  buildClientConfig() {
    const config = {
      region: this.region,
    };

    if (this.accessKeyId && this.secretAccessKey) {
      config.credentials = {
        accessKeyId: this.accessKeyId,
        secretAccessKey: this.secretAccessKey,
      };
      if (this.sessionToken) {
        config.credentials.sessionToken = this.sessionToken;
      }
    }

    return config;
  }

  async getSdkClient() {
    if (this.client) return this.client;
    this.assertReady();

    let sesModule;
    try {
      sesModule = await loadSesModule();
    } catch {
      throw new AmazonSesClientError(
        'No se encontró @aws-sdk/client-sesv2. Instala la dependencia para usar Amazon SES',
        {
          code: 'SES_SDK_NOT_INSTALLED',
          statusCode: 500,
        }
      );
    }

    const { SESv2Client } = sesModule;
    this.client = new SESv2Client(this.buildClientConfig());
    return this.client;
  }

  async getEmailIdentityStatus({
    emailIdentity,
  }) {
    const safeIdentity = cleanString(emailIdentity, 320).toLowerCase();
    if (!safeIdentity) {
      throw new AmazonSesClientError('`emailIdentity` es requerido', {
        code: 'SES_IDENTITY_REQUIRED',
        statusCode: 400,
      });
    }

    try {
      const sesModule = await loadSesModule();
      const { GetEmailIdentityCommand } = sesModule;
      const client = await this.getSdkClient();
      const response = await client.send(new GetEmailIdentityCommand({
        EmailIdentity: safeIdentity,
      }));

      return {
        exists: true,
        emailIdentity: safeIdentity,
        identityType: cleanString(response?.IdentityType || '', 80).toLowerCase(),
        verified: response?.VerifiedForSendingStatus === true,
        verifiedForSendingStatus: response?.VerifiedForSendingStatus === true,
        dkimStatus: cleanString(response?.DkimAttributes?.Status || '', 80).toLowerCase(),
        dkimTokens: extractDkimTokens(response?.DkimAttributes),
        dkimSigningEnabled: response?.DkimAttributes?.SigningEnabled === true,
        feedbackForwardingEnabled: response?.FeedbackForwardingStatus === true,
        mailFromDomain: cleanString(response?.MailFromAttributes?.MailFromDomain || '', 260),
      };
    } catch (error) {
      const mapped = mapSesError(error);
      if (mapped.code === 'SES_IDENTITY_NOT_FOUND') {
        return {
          exists: false,
          emailIdentity: safeIdentity,
          verified: false,
          verifiedForSendingStatus: false,
          identityType: '',
          dkimStatus: '',
          dkimTokens: [],
          dkimSigningEnabled: false,
          feedbackForwardingEnabled: false,
          mailFromDomain: '',
        };
      }
      throw mapped;
    }
  }

  async createEmailIdentity({
    emailIdentity,
  }) {
    const safeIdentity = cleanString(emailIdentity, 320).toLowerCase();
    if (!safeIdentity) {
      throw new AmazonSesClientError('`emailIdentity` es requerido', {
        code: 'SES_IDENTITY_REQUIRED',
        statusCode: 400,
      });
    }

    try {
      const sesModule = await loadSesModule();
      const { CreateEmailIdentityCommand } = sesModule;
      const client = await this.getSdkClient();
      const response = await client.send(new CreateEmailIdentityCommand({
        EmailIdentity: safeIdentity,
      }));

      return {
        created: true,
        emailIdentity: safeIdentity,
        verifiedForSendingStatus: response?.VerifiedForSendingStatus === true,
        dkimStatus: cleanString(response?.DkimAttributes?.Status || '', 80).toLowerCase(),
        dkimTokens: extractDkimTokens(response?.DkimAttributes),
      };
    } catch (error) {
      throw mapSesError(error);
    }
  }

  async ensureDomainIdentity(domain = '') {
    const safeDomain = cleanString(domain, 320).toLowerCase();
    if (!safeDomain) {
      throw new AmazonSesClientError('`domain` es requerido', {
        code: 'SES_DOMAIN_REQUIRED',
        statusCode: 400,
      });
    }

    const current = await this.getEmailIdentityStatus({
      emailIdentity: safeDomain,
    });
    if (current?.exists) {
      return {
        created: false,
        alreadyExists: true,
        emailIdentity: safeDomain,
        identityStatus: current,
      };
    }

    try {
      await this.createEmailIdentity({
        emailIdentity: safeDomain,
      });
    } catch (error) {
      if (!(error instanceof AmazonSesClientError) || error.code !== 'SES_IDENTITY_ALREADY_EXISTS') {
        throw error;
      }
    }

    const status = await this.getEmailIdentityStatus({
      emailIdentity: safeDomain,
    });

    return {
      created: true,
      alreadyExists: false,
      emailIdentity: safeDomain,
      identityStatus: status,
    };
  }

  async sendEmail({
    fromEmail,
    toEmails,
    ccEmails,
    bccEmails,
    replyToEmails,
    subject,
    textBody,
    htmlBody,
    configurationSetName,
    tags,
  }) {
    const safeFrom = cleanString(fromEmail, 280).toLowerCase();
    const to = toStringArray(toEmails).map((item) => item.toLowerCase());
    const cc = toStringArray(ccEmails).map((item) => item.toLowerCase());
    const bcc = toStringArray(bccEmails).map((item) => item.toLowerCase());
    const replyTo = toStringArray(replyToEmails).map((item) => item.toLowerCase());
    const safeSubject = cleanString(subject, 220);
    const safeTextBody = String(textBody ?? '').trim();
    const safeHtmlBody = String(htmlBody ?? '').trim();
    const safeConfigurationSetName = cleanString(configurationSetName, 120);

    if (!safeFrom) {
      throw new AmazonSesClientError('`fromEmail` es requerido para enviar con SES', {
        code: 'SES_FROM_REQUIRED',
        statusCode: 400,
      });
    }

    if (to.length === 0) {
      throw new AmazonSesClientError('Se requiere al menos un destinatario en `toEmails`', {
        code: 'SES_TO_REQUIRED',
        statusCode: 400,
      });
    }

    if (!safeSubject) {
      throw new AmazonSesClientError('`subject` es requerido', {
        code: 'SES_SUBJECT_REQUIRED',
        statusCode: 400,
      });
    }

    if (!safeTextBody && !safeHtmlBody) {
      throw new AmazonSesClientError('Incluye `textBody` o `htmlBody` para enviar el correo', {
        code: 'SES_BODY_REQUIRED',
        statusCode: 400,
      });
    }

    const destination = {
      ToAddresses: to,
    };
    if (cc.length > 0) destination.CcAddresses = cc;
    if (bcc.length > 0) destination.BccAddresses = bcc;

    const body = {};
    if (safeTextBody) {
      body.Text = {
        Charset: 'UTF-8',
        Data: safeTextBody,
      };
    }
    if (safeHtmlBody) {
      body.Html = {
        Charset: 'UTF-8',
        Data: safeHtmlBody,
      };
    }

    const emailTags = Array.isArray(tags)
      ? tags
        .map((item) => ({
          Name: cleanString(item?.name || item?.Name || '', 256),
          Value: cleanString(item?.value || item?.Value || '', 256),
        }))
        .filter((item) => item.Name && item.Value)
      : [];

    const input = {
      FromEmailAddress: safeFrom,
      Destination: destination,
      Content: {
        Simple: {
          Subject: {
            Charset: 'UTF-8',
            Data: safeSubject,
          },
          Body: body,
        },
      },
    };

    if (replyTo.length > 0) {
      input.ReplyToAddresses = replyTo;
    }
    if (safeConfigurationSetName) {
      input.ConfigurationSetName = safeConfigurationSetName;
    }
    if (emailTags.length > 0) {
      input.EmailTags = emailTags;
    }

    try {
      const sesModule = await loadSesModule();
      const { SendEmailCommand } = sesModule;
      const client = await this.getSdkClient();
      const response = await client.send(new SendEmailCommand(input));
      return {
        messageId: cleanString(response?.MessageId || '', 180),
        fromEmail: safeFrom,
        toEmails: to,
        ccEmails: cc,
        bccEmails: bcc,
      };
    } catch (error) {
      this.logger.error(
        `[amazon-ses] send error from=${safeFrom} to=${to.join(',')}: ${error?.message || error}`
      );
      throw mapSesError(error);
    }
  }
}
