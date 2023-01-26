wit_bindgen_rust::export!("../sqs.wit");

struct Sqs;

impl sqs::Sqs for Sqs {
    fn handle_queue_message(message: sqs::Message,) -> Result<(), sqs::Error> {
        println!("I GOT A MESSAGE ID: {:?}, BODY: {:?}", message.id, message.body);
        Ok(())
    }
}
