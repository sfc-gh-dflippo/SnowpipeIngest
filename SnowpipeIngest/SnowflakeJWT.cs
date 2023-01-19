using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using Microsoft.IdentityModel.Tokens;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;

namespace SnowpipeIngest
{
    class SnowflakeJWT
    {

        static RSACryptoServiceProvider rsaProvider = new();

        /// <summary>
        /// Generates a JwtToken to use for login.
        /// </summary>
        /// <returns>The generated JWT token.</returns>
        public static string GenerateSnowflakeJwtToken(string snowflakeAccount, string snowflakeUserId, string pkPath, string pkPwd)
        {
            // Most of the following code is copied directly from Snowflake's .net driver

            // Extract the public key from the private key to generate the fingerprints
            RSAParameters rsaParams;
            string? publicKeyFingerPrint = null;
            AsymmetricCipherKeyPair? keypair = null;

            using (TextReader tr = new StreamReader(pkPath))
            {
                try
                {
                    PemReader? pr;
                    if (null != pkPwd)
                    {
                        IPasswordFinder ipwdf = new PasswordFinder(pkPwd);
                        pr = new PemReader(tr, ipwdf);
                    }
                    else
                    {
                        pr = new PemReader(tr);
                    }

                    object key = pr.ReadObject();
                    // Infer what the pem reader is sending back based on the object properties
                    if (key.GetType().GetProperty("Private") != null)
                    {
                        // PKCS1 key
                        keypair = (AsymmetricCipherKeyPair)key;
                        rsaParams = DotNetUtilities.ToRSAParameters(
                        keypair.Private as RsaPrivateCrtKeyParameters);
                    }
                    else
                    {
                        // PKCS8 key
                        RsaPrivateCrtKeyParameters pk = (RsaPrivateCrtKeyParameters)key;
                        rsaParams = DotNetUtilities.ToRSAParameters(pk);
                        keypair = DotNetUtilities.GetRsaKeyPair(rsaParams);
                    }
                    if (keypair == null)
                    {
                        throw new Exception("Unknown error.");
                    }
                }
                catch (Exception e)
                {
                    throw new Exception(
                        "Could not read private key", e);
                }
            }

            // Generate the public key fingerprint
            var publicKey = keypair.Public;
            byte[] publicKeyEncoded =
                SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(publicKey).GetDerEncoded();
            using (SHA256 SHA256Encoder = SHA256.Create())
            {
                byte[] sha256Hash = SHA256Encoder.ComputeHash(publicKeyEncoded);
                publicKeyFingerPrint = "SHA256:" + Convert.ToBase64String(sha256Hash);
            }

            // Generating the token 
            var now = DateTime.UtcNow;
            System.DateTime dtDateTime =
                new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            long secondsSinceEpoch = (long)((now - dtDateTime).TotalSeconds);

            /* 
             * Payload content
             *      iss : $accountName.$userName.$pulicKeyFingerprint
             *      sub : $accountName.$userName
             *      iat : $now
             *      exp : $now + LIFETIME
             * 
             * Note : Lifetime = 120sec for Python impl, 60sec for Jdbc and Odbc
            */
            String accountUser =
                    snowflakeAccount.ToUpper() +
                    "." +
                    snowflakeUserId.ToUpper();
            String issuer = accountUser + "." + publicKeyFingerPrint;
            var claims = new[] {
                        new Claim(
                            JwtRegisteredClaimNames.Iat,
                            secondsSinceEpoch.ToString(),
                            System.Security.Claims.ClaimValueTypes.Integer64),
                        new Claim(JwtRegisteredClaimNames.Sub, accountUser),
                    };

            rsaProvider.ImportParameters(rsaParams);
            var token = new JwtSecurityToken(
                // Issuer
                issuer,
                // Audience
                null,
                // Subject
                claims,
                //NotBefore
                null,
                // Expires
                now.AddSeconds(60),
                //SigningCredentials
                new SigningCredentials(
                    new RsaSecurityKey(rsaProvider), SecurityAlgorithms.RsaSha256)
            );

            // Serialize the jwt token
            // Base64URL-encoded parts delimited by period ('.'), with format :
            //     [header-base64url].[payload-base64url].[signature-base64url]
            var handler = new JwtSecurityTokenHandler();
            string jwtToken = handler.WriteToken(token);

            return jwtToken;
        }

        /// <summary>
        /// Helper class to handle the password for the certificate if there is one.
        /// </summary>
        private class PasswordFinder : IPasswordFinder
        {
            // The password.
            private readonly string password;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="password">The password.</param>
            public PasswordFinder(string password)
            {
                this.password = password;
            }

            /// <summary>
            /// Returns the password or null if the password is empty or null.
            /// </summary>
            /// <returns>The password or null if the password is empty or null.</returns>
            public char[]? GetPassword()
            {
                if ((null == password) || (0 == password.Length))
                {
                    // No password.
                    return null;
                }
                else
                {
                    return password.ToCharArray();
                }
            }
        }
    }
}

