using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Orleans.Networking.Streams;

namespace Orleans.Networking.Security;

public abstract class TlsNetworkTransport : StreamNetworkTransport
{
    private readonly NetworkTransport _innerTransport;
    private readonly TlsOptions _options;
    private readonly ILogger _logger;
    private readonly NetworkTransportStream _networkTransportStream;
    private readonly SslStream _sslStream;

    public TlsNetworkTransport(NetworkTransport transport, TlsOptions options, ILogger logger) : base(logger)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        _innerTransport = transport;

        _options = options;
        _logger = logger;
        _networkTransportStream = new NetworkTransportStream(_innerTransport, _options.MemoryPool);
        _sslStream = new SslStream(
                _networkTransportStream,
                leaveInnerStreamOpen: false,
                userCertificateValidationCallback: (sender, certificate, chain, sslPolicyErrors) =>
                {
                    if (certificate == null)
                    {
                        return _options.RemoteCertificateMode != RemoteCertificateMode.RequireCertificate;
                    }

                    if (_options.RemoteCertificateValidation == null)
                    {
                        if (sslPolicyErrors != SslPolicyErrors.None)
                        {
                            return false;
                        }
                    }

                    var certificate2 = ConvertToX509Certificate2(certificate);
                    if (certificate2 == null)
                    {
                        return false;
                    }

                    if (_options.RemoteCertificateValidation != null)
                    {
                        if (!_options.RemoteCertificateValidation(certificate2, chain, sslPolicyErrors))
                        {
                            return false;
                        }
                    }

                    return true;
                });
    }

    protected TlsOptions Options => _options;

    protected override SslStream Stream => _sslStream;

    public override async ValueTask CloseAsync(Exception? closeException)
    {
        await _innerTransport.CloseAsync(closeException);
        await base.CloseAsync(closeException);
    }

    protected override async Task RunAsyncCore()
    {
        try
        {
            await AuthenticateAsync();
            await base.RunAsyncCore();
        }
        finally
        {
            await DisposeAsync();
        }
    }

    private async Task AuthenticateAsync()
    {
        bool certificateRequired;

        if (_options.RemoteCertificateMode == RemoteCertificateMode.NoCertificate)
        {
            certificateRequired = false;
        }
        else
        {
            certificateRequired = true;
        }

        using (var cancellationTokenSource = new CancellationTokenSource(_options.HandshakeTimeout))
        {
            try
            {
                await AuthenticateAsyncCore(_innerTransport, certificateRequired, cancellationTokenSource.Token);
            }
            catch (OperationCanceledException ex)
            {
                _logger?.LogWarning(2, ex, "Authentication timed out");
                await _sslStream.DisposeAsync();
                await _innerTransport.CloseAsync(ex);
                return;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(1, ex, "Authentication failed");
                await _sslStream.DisposeAsync();
                await _innerTransport.CloseAsync(ex);
                return;
            }
        }
    }

    protected abstract Task AuthenticateAsyncCore(NetworkTransport transport, bool certificateRequired, CancellationToken cancellationToken);

    public override async ValueTask DisposeAsync()
    {
        await _sslStream.DisposeAsync();
        await _networkTransportStream.DisposeAsync();
        await _innerTransport.DisposeAsync();
        await base.DisposeAsync();
    }

    private static X509Certificate2? ConvertToX509Certificate2(X509Certificate? certificate)
    {
        if (certificate is null)
        {
            return null;
        }

        return certificate as X509Certificate2 ?? new X509Certificate2(certificate);
    }
}
