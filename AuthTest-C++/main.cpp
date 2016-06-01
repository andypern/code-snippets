#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>

using namespace Aws;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Auth;
using namespace Aws::Client;

static const char* KEY = "KEYNAME";
static const char* BUCKET = "BUCKETNAME";
static const char* CONTENT = "This is a sample content";

int main() {
	SDKOptions options;
	options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
	Aws::InitAPI(options);

	ClientConfiguration conf = ClientConfiguration();
	conf.endpointOverride = "IPADDRESS";
	conf.verifySSL = false;
	AWSCredentials creds = AWSCredentials("ACCESS_KEY_ID", "ACCESS_KEY_SECRET");

    S3Client client = S3Client(creds, conf);

	// Put an object
	PutObjectRequest putObjectRequest;
	putObjectRequest.WithKey(KEY).WithBucket(BUCKET).WithContentEncoding("text");

	auto requestStream = Aws::MakeShared<Aws::StringStream>("s3-sample");
	*requestStream << CONTENT;
	putObjectRequest.SetBody(requestStream);
	auto putObjectOutcome = client.PutObject(putObjectRequest);
	if(putObjectOutcome.IsSuccess()) {
		std::cout << "Putting to '" << BUCKET << "/" << KEY << "' succeeded" << std::endl;
	} else {
		std::cout << "Error while putting Object " << putObjectOutcome.GetError().GetExceptionName() << 
			" " << putObjectOutcome.GetError().GetMessage() << std::endl;
		return 1;
	}

	// Retrieve the object
	GetObjectRequest getObjectRequest;
	getObjectRequest = getObjectRequest.WithBucket(BUCKET).WithKey(KEY);
	auto getObjectOutcome = client.GetObject(getObjectRequest);
	if(getObjectOutcome.IsSuccess()) {
		Aws::StringStream contents;
		contents << getObjectOutcome.GetResult().GetBody().rdbuf();

		std::cout << "Successfully retrieved '" << BUCKET << "/" << KEY << "' ";
		std::cout << "with contents: '" << contents.str() << "'" << std::endl;
		if (contents.str().compare(CONTENT) != 0 ){
			std::cout << "Retrieved content is not as expected" << std::endl;
			return 1;
		} else {
			std::cout << "Retrieved content is as expected" << std::endl;
		}
	} else {
		std::cout << "Error while getting object " << getObjectOutcome.GetError().GetExceptionName() <<
			" " << getObjectOutcome.GetError().GetMessage() << std::endl;
		return 1;
	}

	return 0;
}
