#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/transfer/S3FileRequest.h>
#include <aws/transfer/UploadFileRequest.h>
#include <aws/transfer/DownloadFileRequest.h>
#include <iostream>
#include <fstream>
#include <chrono>
#include <string>

using namespace std;
using ns = chrono::nanoseconds;
using get_time = chrono::steady_clock;

using namespace Aws;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::Transfer;


static const char* FILENAME = "big.file";
//static const char* KEY = "big.file";
static const char* BUCKET = "apcontainer";
//static const char* CONTENT = "This is a sample content";



int main(int argc,char *argv[]) {
	SDKOptions options;
	options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
	Aws::InitAPI(options);

	//parse CLI opts

	if(argc == 1){
		std::cout<<"No args, try again"<<endl;
		exit(1);
	}
	else {
		static const char* ACCESS_KEY_ID = argv[1];
		static const char* ACCESS_KEY_SECRET = argv[2];
	}


	ClientConfiguration conf = ClientConfiguration();
	conf.scheme = Aws::Http::Scheme::HTTP;
	conf.endpointOverride = "demo.iggy.bz:7070";
	conf.verifySSL = false;
	AWSCredentials creds = AWSCredentials("ACCESS_KEY_ID", "ACCESS_KEY_SECRET");

    S3Client client = S3Client(creds, conf);

	// Upload a file
/*
 	UploadFileRequest uploadFileRequest = new UploadFileRequest
 	(
 		FILENAME,
 		BUCKET,
 		KEY,
 		TEXT,
 		client,
 		false,
 		false
 	);
*/


//loop start

 	for ( int x = 0; x < 10; x++) {
 		//make our int a string so we can increment keyname
	    std::string str = std::to_string(x);
        char const *KEY = str.c_str();


 	

		// Put an object
		//start our timer
		auto start = get_time::now();


		PutObjectRequest putObjectRequest;
		putObjectRequest.WithKey(KEY).WithBucket(BUCKET).WithContentEncoding("text");

		Aws::IFStream inputFile(FILENAME);
		auto requestStream = Aws::MakeShared<Aws::StringStream>("s3-sample");
		
		*requestStream << inputFile.rdbuf();

		//*requestStream << CONTENT;
		putObjectRequest.SetBody(requestStream);
		auto putObjectOutcome = client.PutObject(putObjectRequest);
		//end our timer
		auto end = get_time::now();
		auto diff = end - start;
		std::cout <<"Elapsed time is :  "<< chrono::duration_cast<ns>(diff).count()<<" ns "<<endl;
		if(putObjectOutcome.IsSuccess()) {
			std::cout << "Putting to '" << BUCKET << "/" << KEY << "' succeeded" << std::endl;
		} else {
			std::cout << "Error while putting Object " << putObjectOutcome.GetError().GetExceptionName() << 
				" " << putObjectOutcome.GetError().GetMessage() << std::endl;
			return 1;
		}
	}

	

	return 0;
}
