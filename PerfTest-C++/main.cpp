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
static const char* BUCKET = "apcontainer";
static const char* CONTENT = "This is a sample content";

int main() {
	SDKOptions options;
	options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
	Aws::InitAPI(options);

	ClientConfiguration conf = ClientConfiguration();
	conf.endpointOverride = "demo.iggy.bz:7070";
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

	

	return 0;
}
