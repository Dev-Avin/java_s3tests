import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

// Legacy V1 imports
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.IOUtils;

//S3 v2 imports 
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;

// Auth and Regions
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

// HTTP Client (Crucial for RGW compatibility)
import software.amazon.awssdk.http.apache.ApacheHttpClient;

// S3 v2 Transfer Manager
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.DirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.DirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;

import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.nio.file.Paths;

public class S3 {

	final static Logger logger = LogManager.getRootLogger();

	private static S3 instance = null;

	protected S3() {
	}

	public static S3 getInstance() {
		if (instance == null) {
			instance = new S3();
		}
		return instance;
	}

	private Properties loadProperties() {
		Properties prop = new Properties();
		try {
			InputStream input = new FileInputStream("config.properties");
			try {
				prop.load(input);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return prop;
	}

	private Properties prop = loadProperties();

	public AmazonS3 getS3Client(Boolean isV4SignerType) {
		String accessKey = prop.getProperty("access_key");
		String secretKey = prop.getProperty("access_secret");
		boolean issecure = Boolean.parseBoolean(prop.getProperty("is_secure"));

		AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(
				new BasicAWSCredentials(accessKey, secretKey));
		EndpointConfiguration epConfig = new AwsClientBuilder.EndpointConfiguration(prop.getProperty("endpoint"),
				prop.getProperty("region"));
		ClientConfiguration clientConfig = new ClientConfiguration();
		if (isV4SignerType) {
			clientConfig.setSignerOverride("AWSS3V4SignerType");
		} else {
			clientConfig.setSignerOverride("S3SignerType");
		}
		if (issecure) {
			clientConfig.setProtocol(Protocol.HTTPS);
		} else {
			clientConfig.setProtocol(Protocol.HTTP);
		}

		clientConfig.setClientExecutionTimeout(900 * 1000);
		clientConfig.setRequestTimeout(60 * 1000);
		clientConfig.withConnectionTimeout(900 * 1000);
		clientConfig.withSocketTimeout(900 * 1000);
		clientConfig.withConnectionMaxIdleMillis(1 * 1000);
		// Allow as many retries as possible until the client executiaon timeout expires
		clientConfig.setMaxErrorRetry(Integer.MAX_VALUE);

		logger.info(String.format("EP is_secure: %s - %b %n", prop.getProperty("endpoint"), issecure));

		AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(credentials)
				.withEndpointConfiguration(epConfig).withClientConfiguration(clientConfig).enablePathStyleAccess()
				.build();
		return s3client;
	}

	// --- MODERN SDK v2 Client ---
	public S3Client getS3V2Client(Boolean isV4SignerType) {
		String accessKey = prop.getProperty("access_key").trim();
		String secretKey = prop.getProperty("access_secret").trim();
		String endpoint = prop.getProperty("endpoint").trim();
		String region = prop.getProperty("region", "us-east-1");
		ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
				.connectionTimeout(Duration.ofMillis(900 * 1000))
				.socketTimeout(Duration.ofMillis(900 * 1000))
				.connectionMaxIdleTime(Duration.ofMillis(1000));

		ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
				.apiCallTimeout(Duration.ofMillis(900 * 1000))
				.apiCallAttemptTimeout(Duration.ofMillis(60 * 1000))
				.retryPolicy(RetryPolicy.builder()
						.numRetries(Integer.MAX_VALUE)
						.build())
				.build();

		S3Configuration s3Config = S3Configuration.builder()
				.pathStyleAccessEnabled(true)
				.build();

		return S3Client.builder()
				.endpointOverride(java.net.URI.create(endpoint))
				.credentialsProvider(StaticCredentialsProvider.create(
						AwsBasicCredentials.create(accessKey, secretKey)))
				.region(Region.of(region))
				.httpClientBuilder(httpClientBuilder)
				.overrideConfiguration(overrideConfig)
				.serviceConfiguration(s3Config)
				.build();
	}

	public String getPrefix() {
		String prefix;
		if (prop.getProperty("bucket_prefix") != null) {
			prefix = prop.getProperty("bucket_prefix");
		} else {
			prefix = "test-";
		}
		return prefix;
	}

	public String getBucketName(String prefix) {
		Random rand = new Random();
		int num = rand.nextInt(50);
		String randomStr = UUID.randomUUID().toString();

		return prefix + randomStr + num;
	}

	public String getBucketName() {
		String prefix = getPrefix();
		Random rand = new Random();
		int num = rand.nextInt(50);
		String randomStr = UUID.randomUUID().toString();

		return prefix + randomStr + num;
	}

	public String repeat(String str, int count) {
		if (count <= 0) {
			return "";
		}
		return new String(new char[count]).replace("\0", str);
	}

	public Boolean isEPSecure() {
		return Boolean.parseBoolean(prop.getProperty("is_secure"));
	}

	public int teradownRetries = 0;
	public int teradownRetriesV2 = 0;

	public void tearDown(AmazonS3 svc) {
		if (teradownRetries > 0) {
			try {
				Thread.sleep(2500);
			} catch (InterruptedException e) {

			}
		}
		try {
			logger.info("TEARDOWN");
			List<Bucket> buckets = svc.listBuckets(new ListBucketsRequest());
			logger.info(String.format("Buckets list size: %d ", buckets.size()));
			String prefix = getPrefix();

			for (Bucket b : buckets) {
				String bucket_name = b.getName();
				if (b.getName().startsWith(prefix)) {
					VersionListing version_listing = svc
							.listVersions(new ListVersionsRequest().withBucketName(bucket_name));
					while (true) {
						for (java.util.Iterator<S3VersionSummary> iterator = version_listing.getVersionSummaries()
								.iterator(); iterator.hasNext();) {
							S3VersionSummary vs = (S3VersionSummary) iterator.next();
							logger.info(String.format("Deleting bucket/object/version: %s / %s / %s", bucket_name,
									vs.getKey(), vs.getVersionId()));
							try {
								svc.deleteVersion(bucket_name, vs.getKey(), vs.getVersionId());
							} catch (AmazonServiceException e) {

							} catch (SdkClientException e) {

							}
						}
						if (version_listing.isTruncated()) {
							version_listing = svc.listNextBatchOfVersions(version_listing);
						} else {
							break;
						}
					}

					ObjectListing object_listing = svc.listObjects(b.getName());
					while (true) {
						for (java.util.Iterator<S3ObjectSummary> iterator = object_listing.getObjectSummaries()
								.iterator(); iterator.hasNext();) {
							S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
							logger.info(
									String.format("Deleting bucket/object: %s / %s", bucket_name, summary.getKey()));
							try {
								svc.deleteObject(bucket_name, summary.getKey());
							} catch (AmazonServiceException e) {

							} catch (SdkClientException e) {

							}
						}
						if (object_listing.isTruncated()) {
							object_listing = svc.listNextBatchOfObjects(object_listing);
						} else {
							break;
						}
					}
					try {
						svc.deleteBucket(new DeleteBucketRequest(b.getName()));
						logger.info(String.format("Deleted bucket: %s", bucket_name));
					} catch (AmazonServiceException e) {

					} catch (SdkClientException e) {

					}
				}
			}
		} catch (AmazonServiceException e) {

		} catch (SdkClientException e) {
			if (teradownRetries < 10) {
				++teradownRetries;
				tearDown(svc);
			}
		}
	}

	public void tearDownV2(S3Client s3Client) {
		if (teradownRetriesV2 > 0) {
			try {
				Thread.sleep(2500);
			} catch (InterruptedException e) {

			}
		}
		try {
			logger.info("TEARDOWN V2");
			ListBucketsResponse bucketsResponse = s3Client.listBuckets();
			List<software.amazon.awssdk.services.s3.model.Bucket> buckets = bucketsResponse.buckets();
			logger.info(String.format("Buckets list size: %d ", buckets.size()));
			String prefix = getPrefix();

			for (software.amazon.awssdk.services.s3.model.Bucket b : buckets) {
				String bucket_name = b.name();
				if (bucket_name.startsWith(prefix)) {
					// Delete all object versions
					try {
						ListObjectVersionsRequest listVersionsReq = ListObjectVersionsRequest.builder()
								.bucket(bucket_name).build();
						ListObjectVersionsResponse versionListing = s3Client.listObjectVersions(listVersionsReq);
						while (true) {
							for (ObjectVersion vs : versionListing.versions()) {
								logger.info(String.format("Deleting bucket/object/version: %s / %s / %s", bucket_name,
										vs.key(), vs.versionId()));
								try {
									s3Client.deleteObject(DeleteObjectRequest.builder()
											.bucket(bucket_name).key(vs.key()).versionId(vs.versionId()).build());
								} catch (S3Exception e) {
								} catch (Exception e) {
								}
							}
							// Also delete delete markers
							for (DeleteMarkerEntry dm : versionListing.deleteMarkers()) {
								logger.info(String.format("Deleting bucket/delete-marker/version: %s / %s / %s",
										bucket_name,
										dm.key(), dm.versionId()));
								try {
									s3Client.deleteObject(DeleteObjectRequest.builder()
											.bucket(bucket_name).key(dm.key()).versionId(dm.versionId()).build());
								} catch (S3Exception e) {
								} catch (Exception e) {
								}
							}
							if (versionListing.isTruncated()) {
								versionListing = s3Client.listObjectVersions(ListObjectVersionsRequest.builder()
										.bucket(bucket_name)
										.keyMarker(versionListing.nextKeyMarker())
										.versionIdMarker(versionListing.nextVersionIdMarker())
										.build());
							} else {
								break;
							}
						}
					} catch (S3Exception e) {
					} catch (Exception e) {
					}

					// Delete remaining objects (non-versioned)
					try {
						ListObjectsV2Request listReq = ListObjectsV2Request.builder()
								.bucket(bucket_name).build();
						ListObjectsV2Response objectListing = s3Client.listObjectsV2(listReq);
						while (true) {
							for (S3Object obj : objectListing.contents()) {
								logger.info(String.format("Deleting bucket/object: %s / %s", bucket_name, obj.key()));
								try {
									s3Client.deleteObject(DeleteObjectRequest.builder()
											.bucket(bucket_name).key(obj.key()).build());
								} catch (S3Exception e) {
								} catch (Exception e) {
								}
							}
							if (objectListing.isTruncated()) {
								objectListing = s3Client.listObjectsV2(ListObjectsV2Request.builder()
										.bucket(bucket_name)
										.continuationToken(objectListing.nextContinuationToken())
										.build());
							} else {
								break;
							}
						}
					} catch (S3Exception e) {
					} catch (Exception e) {
					}

					// Delete the bucket
					try {
						s3Client.deleteBucket(software.amazon.awssdk.services.s3.model.DeleteBucketRequest.builder()
								.bucket(bucket_name).build());
						logger.info(String.format("Deleted bucket: %s", bucket_name));
					} catch (S3Exception e) {
					} catch (Exception e) {
					}
				}
			}
		} catch (S3Exception e) {

		} catch (Exception e) {
			if (teradownRetriesV2 < 10) {
				++teradownRetriesV2;
				tearDownV2(s3Client);
			}
		}
	}

	public String[] EncryptionSseCustomerWrite(AmazonS3 svc, int file_size) {

		String prefix = getPrefix();
		String bucket_name = getBucketName(prefix);
		String key = "key1";
		String data = repeat("testcontent", file_size);
		InputStream datastream = new ByteArrayInputStream(data.getBytes());

		svc.createBucket(bucket_name);

		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(data.length());
		objectMetadata.setContentType("text/plain");
		objectMetadata.setHeader("x-amz-server-side-encryption-customer-key",
				"pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
		objectMetadata.setSSECustomerKeyMd5("DWygnHRtgiJ77HCm+1rvHw==");
		objectMetadata.setSSECustomerAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		PutObjectRequest putRequest = new PutObjectRequest(bucket_name, key, datastream, objectMetadata);

		svc.putObject(putRequest);

		SSECustomerKey skey = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
		GetObjectRequest getRequest = new GetObjectRequest(bucket_name, key);
		getRequest.withSSECustomerKey(skey);

		InputStream inputStream = svc.getObject(getRequest).getObjectContent();
		String rdata = null;
		try {
			rdata = IOUtils.toString(inputStream);
		} catch (IOException e) {
			// e.printStackTrace();
		}

		String arr[] = new String[2];
		arr[0] = data;
		arr[1] = rdata;

		return arr;
	}

	public String[] EncryptionSseCustomerWriteV2(S3Client s3Client, int file_size) {

		String prefix = getPrefix();
		String bucket_name = getBucketName(prefix);
		String key = "key1";
		String data = repeat("testcontent", file_size);

		s3Client.createBucket(b -> b.bucket(bucket_name));

		String sseCustomerKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
		String sseCustomerKeyMd5 = "DWygnHRtgiJ77HCm+1rvHw==";
		String sseCustomerAlgorithm = "AES256";

		software.amazon.awssdk.services.s3.model.PutObjectRequest putRequest = software.amazon.awssdk.services.s3.model.PutObjectRequest
				.builder()
				.bucket(bucket_name)
				.key(key)
				.contentType("text/plain")
				.sseCustomerAlgorithm(sseCustomerAlgorithm)
				.sseCustomerKey(sseCustomerKey)
				.sseCustomerKeyMD5(sseCustomerKeyMd5)
				.build();

		s3Client.putObject(putRequest, RequestBody.fromString(data));

		software.amazon.awssdk.services.s3.model.GetObjectRequest getRequest = software.amazon.awssdk.services.s3.model.GetObjectRequest
				.builder()
				.bucket(bucket_name)
				.key(key)
				.sseCustomerAlgorithm(sseCustomerAlgorithm)
				.sseCustomerKey(sseCustomerKey)
				.sseCustomerKeyMD5(sseCustomerKeyMd5)
				.build();

		ResponseInputStream<GetObjectResponse> responseStream = s3Client.getObject(getRequest);
		String rdata = null;
		try {
			rdata = new String(responseStream.readAllBytes());
		} catch (IOException e) {
			// e.printStackTrace();
		}

		String arr[] = new String[2];
		arr[0] = data;
		arr[1] = rdata;

		return arr;
	}

	public Bucket createKeys(AmazonS3 svc, String[] keys) {
		String prefix = prop.getProperty("bucket_prefix");
		String bucket_name = getBucketName(prefix);
		Bucket bucket = svc.createBucket(bucket_name);

		for (String k : keys) {
			svc.putObject(bucket.getName(), k, k);
		}
		return bucket;
	}

	public String createKeysV2(S3Client s3Client, String[] keys) {
		String prefix = prop.getProperty("bucket_prefix");
		String bucket_name = getBucketName(prefix);
		s3Client.createBucket(b -> b.bucket(bucket_name));

		for (String k : keys) {
			s3Client.putObject(
					software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
							.bucket(bucket_name).key(k).build(),
					RequestBody.fromString(k));
		}
		return bucket_name;
	}

	public CompleteMultipartUploadRequest multipartUploadLLAPI(AmazonS3 svc, String bucket, String key, long size,
			String filePath) {

		List<PartETag> partETags = new ArrayList<PartETag>();

		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
		InitiateMultipartUploadResult initResponse = svc.initiateMultipartUpload(initRequest);

		File file = new File(filePath);
		long contentLength = file.length();
		long partSize = size;

		long filePosition = 0;
		for (int i = 1; filePosition < contentLength; i++) {
			partSize = Math.min(partSize, (contentLength - filePosition));
			UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucket).withKey(key)
					.withUploadId(initResponse.getUploadId()).withPartNumber(i).withFileOffset(filePosition)
					.withFile(file).withPartSize(partSize);

			partETags.add((PartETag) svc.uploadPart(uploadRequest).getPartETag());

			filePosition += partSize;
		}

		CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucket, key,
				initResponse.getUploadId(), (List<PartETag>) partETags);

		return compRequest;
	}

	public software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest multipartUploadLLAPIV2(
			S3Client s3Client, String bucket, String key, long size, String filePath) {

		List<CompletedPart> completedParts = new ArrayList<CompletedPart>();

		CreateMultipartUploadResponse initResponse = s3Client.createMultipartUpload(
				CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build());
		String uploadId = initResponse.uploadId();

		File file = new File(filePath);
		long contentLength = file.length();
		long partSize = size;

		long filePosition = 0;
		for (int i = 1; filePosition < contentLength; i++) {
			partSize = Math.min(partSize, (contentLength - filePosition));

			UploadPartResponse uploadPartResponse;
			try {
				FileInputStream fis = new FileInputStream(file);
				fis.skip(filePosition);
				byte[] partBytes = new byte[(int) partSize];
				fis.read(partBytes);
				fis.close();

				uploadPartResponse = s3Client.uploadPart(
						software.amazon.awssdk.services.s3.model.UploadPartRequest.builder()
								.bucket(bucket).key(key).uploadId(uploadId)
								.partNumber(i).contentLength(partSize).build(),
						RequestBody.fromBytes(partBytes));

				completedParts.add(CompletedPart.builder()
						.partNumber(i).eTag(uploadPartResponse.eTag()).build());
			} catch (IOException e) {
				throw new RuntimeException("Failed to read file part", e);
			}

			filePosition += partSize;
		}

		return software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest.builder()
				.bucket(bucket).key(key).uploadId(uploadId)
				.multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
				.build();
	}

	public CompleteMultipartUploadRequest multipartCopyLLAPI(AmazonS3 svc, String dstbkt, String dstkey, String srcbkt,
			String srckey, long size) {

		InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(dstbkt, dstkey);
		InitiateMultipartUploadResult initResult = svc.initiateMultipartUpload(initiateRequest);
		GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(srcbkt, srckey);

		ObjectMetadata metadataResult = svc.getObjectMetadata(metadataRequest);
		long objectSize = metadataResult.getContentLength(); // in bytes

		long partSize = size;

		long bytePosition = 0;
		int partNum = 1;

		List<PartETag> partETags = new ArrayList<PartETag>();
		while (bytePosition < objectSize) {
			long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);
			CopyPartRequest copyRequest = new CopyPartRequest().withDestinationBucketName(dstbkt)
					.withDestinationKey(dstkey).withSourceBucketName(srcbkt).withSourceKey(srckey)
					.withUploadId(initResult.getUploadId()).withFirstByte(bytePosition).withLastByte(lastByte)
					.withPartNumber(partNum++);

			CopyPartResult res = svc.copyPart(copyRequest);
			partETags.add(res.getPartETag());
			bytePosition += partSize;
		}
		CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(dstbkt, dstkey,
				initResult.getUploadId(), partETags);

		return completeRequest;
	}

	public software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest multipartCopyLLAPIV2(
			S3Client s3Client, String dstbkt, String dstkey, String srcbkt, String srckey, long size) {

		CreateMultipartUploadResponse initResult = s3Client.createMultipartUpload(
				CreateMultipartUploadRequest.builder().bucket(dstbkt).key(dstkey).build());
		String uploadId = initResult.uploadId();

		HeadObjectResponse metadataResult = s3Client.headObject(
				HeadObjectRequest.builder().bucket(srcbkt).key(srckey).build());
		long objectSize = metadataResult.contentLength(); // in bytes

		long partSize = size;

		long bytePosition = 0;
		int partNum = 1;

		List<CompletedPart> completedParts = new ArrayList<CompletedPart>();
		while (bytePosition < objectSize) {
			long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);
			String copySourceRange = "bytes=" + bytePosition + "-" + lastByte;

			UploadPartCopyResponse res = s3Client.uploadPartCopy(
					UploadPartCopyRequest.builder()
							.destinationBucket(dstbkt).destinationKey(dstkey)
							.sourceBucket(srcbkt).sourceKey(srckey)
							.uploadId(uploadId)
							.copySourceRange(copySourceRange)
							.partNumber(partNum).build());

			completedParts.add(CompletedPart.builder()
					.partNumber(partNum).eTag(res.copyPartResult().eTag()).build());
			partNum++;
			bytePosition += partSize;
		}

		return software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest.builder()
				.bucket(dstbkt).key(dstkey).uploadId(uploadId)
				.multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
				.build();
	}

	static List<PartETag> GetETags(List<CopyPartResult> responses) {
		List<PartETag> etags = new ArrayList<PartETag>();
		for (CopyPartResult response : responses) {
			etags.add(new PartETag(response.getPartNumber(), response.getETag()));
		}
		return etags;
	}

	public void waitForCompletion(Transfer xfer) {
		try {
			xfer.waitForCompletion();
		} catch (AmazonServiceException e) {
			// e.printStackTrace();
		} catch (AmazonClientException e) {
			// e.printStackTrace();
		} catch (InterruptedException e) {
			// e.printStackTrace();
		}
	}

	public Copy multipartCopyHLAPI(AmazonS3 svc, String dstbkt, String dstkey, String srcbkt, String srckey) {
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(svc).build();
		Copy copy = tm.copy(srcbkt, srckey, dstbkt, dstkey);
		try {
			waitForCompletion(copy);
		} catch (AmazonServiceException e) {

		}
		return copy;
	}

	public Download downloadHLAPI(AmazonS3 svc, String bucket, String key, File file) {
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(svc).build();
		Download download = tm.download(bucket, key, file);
		try {
			waitForCompletion(download);
		} catch (AmazonServiceException e) {

		}
		return download;
	}

	public MultipleFileDownload multipartDownloadHLAPI(AmazonS3 svc, String bucket, String key, File dstDir) {
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(svc).build();
		MultipleFileDownload download = tm.downloadDirectory(bucket, key, dstDir);
		try {
			waitForCompletion(download);
		} catch (AmazonServiceException e) {

		}
		return download;
	}

	public Upload UploadFileHLAPI(AmazonS3 svc, String bucket, String key, String filePath) {
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(svc)
				.build();
		Upload upload = tm.upload(bucket, key, new File(filePath));
		try {
			waitForCompletion(upload);
		} catch (AmazonServiceException e) {

		}
		return upload;
	}

	public Transfer multipartUploadHLAPI(AmazonS3 svc, String bucket, String s3target, String directory)
			throws AmazonServiceException, AmazonClientException, InterruptedException {

		TransferManager tm = TransferManagerBuilder.standard().withS3Client(svc).build();
		Transfer t = tm.uploadDirectory(bucket, s3target, new File(directory), false);
		try {
			waitForCompletion(t);
		} catch (AmazonServiceException e) {

		}
		return t;
	}

	// --- V2 TransferManager HLAPI methods ---

	public S3AsyncClient getS3V2AsyncClient() {
		String accessKey = prop.getProperty("access_key");
		String secretKey = prop.getProperty("access_secret");
		String endpoint = prop.getProperty("endpoint");
		String region = prop.getProperty("region", "us-east-1");

		return S3AsyncClient.builder()
				.endpointOverride(java.net.URI.create(endpoint))
				.credentialsProvider(StaticCredentialsProvider.create(
						AwsBasicCredentials.create(accessKey, secretKey)))
				.region(Region.of(region))
				.multipartEnabled(true)
				.serviceConfiguration(S3Configuration.builder()
						.pathStyleAccessEnabled(true)
						.build())
				.build();
	}

	private S3TransferManager buildTransferManagerV2(S3AsyncClient s3AsyncClient) {
		return S3TransferManager.builder()
				.s3Client(s3AsyncClient)
				.build();
	}

	public CompletedCopy multipartCopyHLAPIV2(S3AsyncClient s3AsyncClient, String dstbkt, String dstkey, String srcbkt,
			String srckey) {
		S3TransferManager tm = buildTransferManagerV2(s3AsyncClient);
		try {
			CopyObjectRequest copyReq = CopyObjectRequest.builder()
					.sourceBucket(srcbkt).sourceKey(srckey)
					.destinationBucket(dstbkt).destinationKey(dstkey)
					.build();
			software.amazon.awssdk.transfer.s3.model.Copy copy = tm.copy(c -> c.copyObjectRequest(copyReq));
			return copy.completionFuture().join();
		} catch (Exception e) {
			logger.error("multipartCopyHLAPIV2 failed", e);
			return null;
		} finally {
			tm.close();
		}
	}

	public CompletedFileDownload downloadHLAPIV2(S3AsyncClient s3AsyncClient, String bucket, String key, File file) {
		S3TransferManager tm = buildTransferManagerV2(s3AsyncClient);
		try {
			DownloadFileRequest downloadReq = DownloadFileRequest.builder()
					.getObjectRequest(b -> b.bucket(bucket).key(key))
					.destination(file.toPath())
					.build();
			FileDownload download = tm.downloadFile(downloadReq);
			return download.completionFuture().join();
		} catch (Exception e) {
			logger.error("downloadHLAPIV2 failed", e);
			return null;
		} finally {
			tm.close();
		}
	}

	public CompletedDirectoryDownload multipartDownloadHLAPIV2(S3AsyncClient s3AsyncClient, String bucket,
			String prefix, File dstDir) {
		S3TransferManager tm = buildTransferManagerV2(s3AsyncClient);
		try {
			DownloadDirectoryRequest downloadDirReq = DownloadDirectoryRequest.builder()
					.bucket(bucket)
					.listObjectsV2RequestTransformer(l -> l.prefix(prefix))
					.destination(dstDir.toPath())
					.build();
			DirectoryDownload dirDownload = tm.downloadDirectory(downloadDirReq);
			return dirDownload.completionFuture().join();
		} catch (Exception e) {
			logger.error("multipartDownloadHLAPIV2 failed", e);
			return null;
		} finally {
			tm.close();
		}
	}

	public CompletedFileUpload UploadFileHLAPIV2(S3AsyncClient s3AsyncClient, String bucket, String key,
			String filePath) {
		S3TransferManager tm = buildTransferManagerV2(s3AsyncClient);
		try {
			UploadFileRequest uploadReq = UploadFileRequest.builder()
					.putObjectRequest(b -> b.bucket(bucket).key(key))
					.source(Paths.get(filePath))
					.build();
			FileUpload upload = tm.uploadFile(uploadReq);
			return upload.completionFuture().join();
		} catch (Exception e) {
			logger.error("UploadFileHLAPIV2 failed", e);
			return null;
		} finally {
			tm.close();
		}
	}

	public CompletedDirectoryUpload multipartUploadHLAPIV2(S3AsyncClient s3AsyncClient, String bucket, String s3target,
			String directory) {
		S3TransferManager tm = buildTransferManagerV2(s3AsyncClient);
		try {
			UploadDirectoryRequest uploadDirReq = UploadDirectoryRequest.builder()
					.bucket(bucket)
					.s3Prefix(s3target)
					.source(Paths.get(directory))
					.build();
			DirectoryUpload dirUpload = tm.uploadDirectory(uploadDirReq);
			return dirUpload.completionFuture().join();
		} catch (Exception e) {
			logger.error("multipartUploadHLAPIV2 failed", e);
			return null;
		} finally {
			tm.close();
		}
	}

	public void createFile(String fname, long size) {
		Random rand = new Random();
		try {
			File f = new File(fname);
			if (f.exists() && !f.isDirectory()) {
				f.delete();
			}
			try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fname))) {
				long remaining = size;
				byte[] buffer = new byte[1024 * 1024]; // 1MB buffer
				while (remaining > 0) {
					int toWrite = (int) Math.min(remaining, buffer.length);
					rand.nextBytes(buffer);
					bos.write(buffer, 0, toWrite);
					remaining -= toWrite;
				}
			}
		} catch (IOException e) {
			logger.error("Error creating file: " + fname, e);
		}
	}
}
