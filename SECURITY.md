# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Currently supported versions:

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |

## Reporting a Vulnerability

We take the security of NoKV seriously. If you discover a security vulnerability, please follow these steps:

### 1. DO NOT Create a Public GitHub Issue

Please do not report security vulnerabilities through public GitHub issues.

### 2. Report via GitHub Security Advisories

The preferred method is to report vulnerabilities through GitHub's Security Advisory feature:

1. Go to the [Security tab](https://github.com/feichai0017/NoKV/security) of this repository
2. Click "Report a vulnerability"
3. Fill out the advisory form with details

### 3. Alternative: Email Report

If you prefer, you can email security reports to the maintainers. Please include:

- Type of vulnerability
- Full paths of affected source files
- Location of the affected source code (tag/branch/commit or direct URL)
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the vulnerability, including how an attacker might exploit it

### 4. What to Expect

After you submit a vulnerability report:

- We will acknowledge your email within 48 hours
- We will provide a more detailed response within 7 days indicating the next steps
- We will keep you informed about the progress toward a fix and announcement
- We may ask for additional information or guidance

### 5. Disclosure Policy

- Security vulnerabilities will be disclosed in a coordinated manner
- We request that you do not publicly disclose the vulnerability until we have had a chance to address it
- We will credit you in the security advisory unless you prefer to remain anonymous

## Security Best Practices for Users

### 1. Keep Dependencies Updated

Regularly update NoKV and its dependencies to get the latest security patches:

```bash
go get -u github.com/feichai0017/NoKV
go mod tidy
```

### 2. Network Security

When deploying NoKV:

- Use TLS/SSL for all network communication in production
- Restrict network access to NoKV ports using firewalls
- Use strong authentication for the Redis gateway
- Isolate Raft cluster traffic from public networks

### 3. File System Security

- Ensure proper file permissions on data directories
- Use encrypted file systems for sensitive data
- Regularly backup your data with encryption

### 4. Configuration Security

- Do not commit sensitive configuration to version control
- Use environment variables or secret management systems for sensitive values
- Review and minimize exposed ports in production

### 5. Monitoring

- Enable and monitor security-related metrics
- Set up alerts for unusual access patterns
- Regularly review logs for security events

## Known Security Considerations

### 1. Authentication

The Redis gateway currently does not enforce authentication by default. In production:

- Implement network-level security (firewalls, VPNs)
- Consider adding authentication middleware
- Limit access to trusted networks only

### 2. Encryption

- Data is not encrypted at rest by default
- Consider using encrypted file systems
- Future versions may include built-in encryption support

### 3. Resource Limits

- Configure appropriate resource limits to prevent DoS attacks
- Monitor memory and disk usage
- Implement rate limiting at the application or infrastructure level

## Security Updates

Security updates will be:

- Released as soon as possible after verification
- Documented in GitHub Security Advisories
- Mentioned in release notes with CVE numbers when applicable
- Announced in the project README and documentation

## Comments on This Policy

If you have suggestions on how this process could be improved, please submit a pull request or open an issue.

---

**Last Updated**: December 2024
