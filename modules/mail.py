class Mail:
    def __init__(self, smtp_server, addr_from, addr_to, subject, password):
        import smtplib
        self.addr_from = addr_from
        self.smtp_server = smtp_server

        self.server = smtplib.SMTP(smtp_server, 587)
        self.server.starttls()
        self.server.login(addr_from, password)

    def send(self, addr_from, addr_to, subject, body):
        to_addr = addr_to
        subject = subject
        body = body

        self.server.sendmail(self.addr_from, to_addr, f"Subject: {subject}\n\n{body}")
        self.server.quit()