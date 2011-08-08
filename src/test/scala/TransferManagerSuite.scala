import org.scalatest.FunSuite
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.model._
import java.util.concurrent.atomic.AtomicInteger
import java.io._

final class TransferManagerSuite extends FunSuite
{
  val credentials = new BasicAWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"),
    System.getenv("AWS_SECRET_ACCESS_KEY"))
  val bucketName = System.getenv("AWS_S3_BUCKET_NAME")

  test("receive ProgressEvent.COMPLETED_EVENT_CODE at most once") {
    val transferManager = new TransferManager(credentials)

    try {
      val metadata = new ObjectMetadata
      metadata.setContentType("text/plain")
      val numCompletedCallbacks = new AtomicInteger(0)
      val progressListener: ProgressListener = new ProgressListener {
        def progressChanged(progressEvent: ProgressEvent): Unit = {
          progressEvent.getEventCode match {
            case ProgressEvent.COMPLETED_EVENT_CODE =>
              numCompletedCallbacks.incrementAndGet()
            case _: Int => ()
          }
        }
      }

      val inputStream = new ByteArrayInputStream(Array[Byte](1, 2, 3, 4))
      try {
        val putRequest = new PutObjectRequest(bucketName, "ReceiveCallbackOnceTest", inputStream, metadata).
          withCannedAcl(CannedAccessControlList.Private).withProgressListener(progressListener)
        val upload = transferManager.upload(putRequest)
        upload.waitForUploadResult()
        Thread.sleep(2000)
        assert(numCompletedCallbacks.get() == 1, numCompletedCallbacks.get())
      } catch {
        case e: Exception =>
          inputStream.close()
          throw e
      }
    } finally {
      transferManager.shutdownNow()
    }
  }

  test("receive only known event codes") {
    val transferManager = new TransferManager(credentials)

    try {
      val metadata = new ObjectMetadata
      metadata.setContentType("text/plain")
      val numUnknownEventCodes = new AtomicInteger(0)
      val progressListener: ProgressListener = new ProgressListener {
        def progressChanged(progressEvent: ProgressEvent): Unit = {
          // http://docs.amazonwebservices.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ProgressEvent.html
          progressEvent.getEventCode match {
            case ProgressEvent.CANCELED_EVENT_CODE => ()
            case ProgressEvent.COMPLETED_EVENT_CODE => ()
            case ProgressEvent.FAILED_EVENT_CODE => ()
            case ProgressEvent.PART_COMPLETED_EVENT_CODE => ()
            case ProgressEvent.PART_FAILED_EVENT_CODE => ()
            case ProgressEvent.PART_STARTED_EVENT_CODE => ()
            case ProgressEvent.STARTED_EVENT_CODE => ()
            case x: Int => println("Unknown event code: " + x); numUnknownEventCodes.incrementAndGet()
          }
        }
      }

      val inputStream = new ByteArrayInputStream(Array.fill(131072){ 1 })
      try {
        val putRequest = new PutObjectRequest(bucketName, "ReceiveCallbackOnceTest", inputStream, metadata).
          withCannedAcl(CannedAccessControlList.Private).withProgressListener(progressListener)
        val upload = transferManager.upload(putRequest)
        upload.waitForUploadResult()
        Thread.sleep(2000)
        assert(numUnknownEventCodes.get() == 0, numUnknownEventCodes.get())
      } catch {
        case e: Exception =>
          inputStream.close()
          throw e
      }
    } finally {
      transferManager.shutdownNow()
    }
  }
}
