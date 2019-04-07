import com.amazonaws.auth.AWSCredentials;

public class GeneratePresignedURL {
    public static void main(String[] args) {
        String clientRegion = "us-east-1";
        String bucketName = "test1234vamsi";
        String objectKey = "test.txt";

    }
    public class AWSCredentialProvider implements AWSCredentials {

        @Override
        public String getAWSAccessKeyId() {
            return "AKIAJP7GLCAC7LHJVCAA";
        }

        @Override
        public String getAWSSecretKey() {
            return "DIbU53mHdVAHOS9X+40mXm2xY8xddzJHIKaLL9eS";
        }

    }
}
