/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.security;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXRevocationChecker;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import static org.neo4j.driver.internal.util.CertificateTool.loadX509Cert;

/**
 * A SecurityPlan consists of encryption and trust details.
 */
public class SecurityPlanImpl implements SecurityPlan
{
    public static SecurityPlan forAllCertificates( boolean requiresHostnameVerification, boolean requiresRevocationChecking ) throws GeneralSecurityException
    {
        SSLContext sslContext = SSLContext.getInstance( "TLS" );
        sslContext.init( new KeyManager[0], new TrustManager[]{new TrustAllTrustManager()}, null );

        return new SecurityPlanImpl( true, sslContext, requiresHostnameVerification, requiresRevocationChecking );
    }

    public static SecurityPlan forCustomCASignedCertificates( File certFile, boolean requiresHostnameVerification,
                                                              boolean requiresRevocationChecking )
            throws GeneralSecurityException, IOException
    {
        SSLContext sslContext = configureSSLContext( certFile, requiresRevocationChecking );
        return new SecurityPlanImpl( true, sslContext, requiresHostnameVerification, requiresRevocationChecking );
    }

    public static SecurityPlan forSystemCASignedCertificates( boolean requiresHostnameVerification, boolean requiresRevocationChecking )
            throws GeneralSecurityException, IOException
    {
        SSLContext sslContext = configureSSLContext( null, requiresRevocationChecking );
        return new SecurityPlanImpl( true, sslContext, requiresHostnameVerification, requiresRevocationChecking );
    }

    private static SSLContext configureSSLContext( File customCertFile, boolean requiresRevocationChecking )
            throws GeneralSecurityException, IOException
    {
        KeyStore trustedKeyStore = KeyStore.getInstance( KeyStore.getDefaultType() );
        trustedKeyStore.load( null, null );

        if ( customCertFile != null )
        {
            // A certificate file is specified so we will load the certificates in the file
            loadX509Cert( customCertFile, trustedKeyStore );
        }
        else
        {
            loadSystemCertificates( trustedKeyStore );
        }

        // Configure certificate revocation checking (X509CertSelector() selects all certificates)
        PKIXBuilderParameters pkixBuilderParameters = new PKIXBuilderParameters( trustedKeyStore, new X509CertSelector() );

        // sets checking of stapled ocsp response
        if ( requiresRevocationChecking )
        {
            CertPathValidator certPathValidator = CertPathValidator.getInstance( "PKIX");
            PKIXRevocationChecker defaultRevocationChecker = (PKIXRevocationChecker)certPathValidator.getRevocationChecker();



            pkixBuilderParameters.setRevocationEnabled(false);
            pkixBuilderParameters.addCertPathChecker( new MustStapleRevocationChecker( defaultRevocationChecker ) );
        }

        // enables status_request extension in client hello
        if ( requiresRevocationChecking )
        {
            System.setProperty( "jdk.tls.client.enableStatusRequestExtension", "true" );
        }

        SSLContext sslContext = SSLContext.getInstance( "TLS" );

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() );
        trustManagerFactory.init( new CertPathTrustManagerParameters( pkixBuilderParameters ) );
        sslContext.init( new KeyManager[0], trustManagerFactory.getTrustManagers(), null );

        return sslContext;
    }

    private static void loadSystemCertificates( KeyStore trustedKeyStore ) throws GeneralSecurityException, IOException
    {
        // To customize the PKIXParameters we need to get hold of the default KeyStore, no other elegant way available
        TrustManagerFactory tempFactory = TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() );
        tempFactory.init( (KeyStore) null );

        // Get hold of the default trust manager
        X509TrustManager x509TrustManager = null;
        for ( TrustManager trustManager : tempFactory.getTrustManagers() )
        {
            if ( trustManager instanceof X509TrustManager )
            {
                x509TrustManager = (X509TrustManager) trustManager;
                break;
            }
        }

        if ( x509TrustManager == null )
        {
            throw new CertificateException( "No system certificates found" );
        }
        else
        {
            // load system default certificates into KeyStore
            loadX509Cert( x509TrustManager.getAcceptedIssuers(), trustedKeyStore );
        }
    }

    public static SecurityPlan insecure()
    {
        return new SecurityPlanImpl( false, null, false, false );
    }

    private final boolean requiresEncryption;
    private final SSLContext sslContext;
    private final boolean requiresHostnameVerification;
    private final boolean requiresRevocationChecking;

    private SecurityPlanImpl( boolean requiresEncryption, SSLContext sslContext, boolean requiresHostnameVerification, boolean requiresRevocationChecking )
    {
        this.requiresEncryption = requiresEncryption;
        this.sslContext = sslContext;
        this.requiresHostnameVerification = requiresHostnameVerification;
        this.requiresRevocationChecking = requiresRevocationChecking;
    }

    @Override
    public boolean requiresEncryption()
    {
        return requiresEncryption;
    }

    @Override
    public SSLContext sslContext()
    {
        return sslContext;
    }

    @Override
    public boolean requiresHostnameVerification()
    {
        return requiresHostnameVerification;
    }

    @Override
    public boolean requiresRevocationChecking()
    {
        return requiresRevocationChecking;
    }

    private static class TrustAllTrustManager implements X509TrustManager
    {
        public void checkClientTrusted( X509Certificate[] chain, String authType ) throws CertificateException
        {
            throw new CertificateException( "All client connections to this client are forbidden." );
        }

        public void checkServerTrusted( X509Certificate[] chain, String authType ) throws CertificateException
        {
            // all fine, pass through
        }

        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }
    }

    private static class MustStapleRevocationChecker extends PKIXRevocationChecker
    {
        private final PKIXRevocationChecker delegatedChecker;
        private final String MUST_STAPLE_OID = "1.3.6.1.5.5.7.1.24";
        private final Set<String> supportedExtensions = Collections.singleton( MUST_STAPLE_OID );

        MustStapleRevocationChecker( PKIXRevocationChecker delegatedChecker )
        {
            super();
            this.delegatedChecker = delegatedChecker;
        }

        @Override
        public void init( boolean forward ) throws CertPathValidatorException
        {
            System.out.println("Init");
            delegatedChecker.init( forward );
        }

        @Override
        public boolean isForwardCheckingSupported()
        {
            return false;
        }

        @Override
        public Set<String> getSupportedExtensions()
        {
            return supportedExtensions;
        }

        @Override
        public void check( Certificate cert, Collection<String> unresolvedCritExts ) throws CertPathValidatorException
        {
            X509Certificate x509Certificate = (X509Certificate) cert;
            byte[] mustStapleExtension = x509Certificate.getExtensionValue( MUST_STAPLE_OID );

            // if we see must staple extension but no valid ocsp response then fail
            if ( mustStapleExtension != null )
            {
                System.out.println( "Must staple found");
            }
            // otherwise validate if we have a stapled response
            else
            {
                System.out.println( delegatedChecker.getOcspResponses().size() );
                System.out.println( "BRuh!!!!!" );
                delegatedChecker.check( cert );
            }
            System.out.println( "Checking!!!!!" );
        }

        @Override
        public List<CertPathValidatorException> getSoftFailExceptions()
        {
            return null;
        }
    }
}
