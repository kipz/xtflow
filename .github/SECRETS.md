# GitHub Secrets Configuration

This document describes the GitHub repository secrets required for automated releases and Clojars deployment.

## Required Secrets

The following secrets must be configured in your GitHub repository settings under **Settings → Secrets and variables → Actions**.

### 1. CLOJARS_USERNAME

Your Clojars deploy token username.

**How to get it:**
1. Log in to [Clojars.org](https://clojars.org)
2. Go to your profile → Deploy Tokens
3. Create a new deploy token
4. Copy the **username** (not your actual username)

### 2. CLOJARS_PASSWORD

Your Clojars deploy token password.

**How to get it:**
1. When creating the deploy token (see above)
2. Copy the **token/password** shown
3. ⚠️ Save it immediately - it won't be shown again!

### 3. GPG_PRIVATE_KEY

Your GPG private key in ASCII-armored format, base64 encoded.

**How to create and export:**

```bash
# Generate a new GPG key (if you don't have one)
gpg --full-generate-key
# Choose: RSA and RSA, 4096 bits, no expiration (or your preference)
# Enter your name and email

# List your keys
gpg --list-secret-keys --keyid-format=long

# Export the private key (replace KEY_ID with your key ID)
gpg --armor --export-secret-keys KEY_ID | base64 -w 0

# Copy the output and paste it as the GPG_PRIVATE_KEY secret
```

### 4. GPG_PASSPHRASE

The passphrase you used when creating your GPG key.

**Note:** If you created a key without a passphrase, you can leave this secret empty, but this is not recommended for security.

## Setting the Secrets

1. Navigate to your repository on GitHub
2. Go to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add each secret with its name and value
5. Click **Add secret**

## Verifying Your Setup

After setting up the secrets:

1. Make a commit to the `main` branch
2. Check the **Actions** tab to see if workflows run successfully
3. The draft release should be created/updated automatically
4. When ready to release, publish the draft release
5. The release workflow will build, sign, and deploy to Clojars

## Troubleshooting

### Deploy fails with "Unauthorized"
- Check that your Clojars deploy token is correct
- Ensure the token has deployment permissions
- Verify both username AND password are from the deploy token, not your login credentials

### GPG signing fails
- Ensure the GPG key is exported correctly (ASCII-armored format)
- Verify the passphrase matches the key
- Check that the key hasn't expired
- Make sure you base64 encoded the entire armored key including headers

### Build fails
- Check that Java and Clojure versions in workflows match your local environment
- Ensure all tests pass locally before releasing
- Verify deps.edn has the correct build tool versions

## Security Best Practices

- ✅ Use deploy tokens instead of your main Clojars password
- ✅ Use a strong passphrase for your GPG key
- ✅ Never commit secrets to the repository
- ✅ Rotate deploy tokens periodically
- ✅ Limit deploy token scope to only necessary repositories
- ✅ Keep your GPG private key secure and backed up

## Resources

- [Clojars Deploy Tokens Documentation](https://github.com/clojars/clojars-web/wiki/Deploy-Tokens)
- [GitHub Secrets Documentation](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- [GPG Quick Start Guide](https://docs.github.com/en/authentication/managing-commit-signature-verification/generating-a-new-gpg-key)
