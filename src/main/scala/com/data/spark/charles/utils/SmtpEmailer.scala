package com.data.spark.charles.utils

import javax.mail.Message;

import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.Address
import javax.mail.internet.MimeMessage;

import java.util.Properties


class SmtpEmailer {
  
  val email_address = scala.util.Properties.envOrElse("EMAIL_ADDRESS", "xxx@yyy.com")
  val email_password = scala.util.Properties.envOrElse("EMAIL_PASSWORD", "@126tyr")
  
  def sendMail(recipientAddr: String, mailBody: String): Unit = {
    
    var props: Properties  = getGmailProperties();
		
		var session: Session  = Session.getDefaultInstance(props,
			new javax.mail.Authenticator() {
		  	override def getPasswordAuthentication(): PasswordAuthentication = {
					return new PasswordAuthentication(email_address,email_password);
				}
			});

		

			var message: Message  = new MimeMessage(session);
			message.setFrom(new InternetAddress(email_address));
			message.setRecipients(Message.RecipientType.TO,buildInternetAddressArray(recipientAddr).asInstanceOf[Array[Address]]);
			message.setSubject("Loan Application");
			message.setText(mailBody);

			Transport.send(message);

			System.out.println("Done");

		
  }
  
  
  def buildInternetAddressArray(address: String): Array[InternetAddress] = {
    return InternetAddress.parse(address)
  }
  
  def getYahooProperties(){
    var props: Properties  = new Properties();
		props.put("mail.smtp.host", "smtp.mail.yahoo.com");
    props.put("mail.stmp.user", email_address);
    props.put("mail.smtp.auth", "true");
    props.put("mail.smtp.starttls.enable", "true");
    props.put("mail.smtp.password", email_password);
    props.put("mail.transport.protocol","smtp");
    props.put("mail.smtp.port", "587");
		props
  }
  
  def getGmailProperties(): Properties = {
    var props: Properties  = new Properties();
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.socketFactory.port", "465");
		props.put("mail.smtp.socketFactory.class",
				"javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.port", "465");
		props
  }
}