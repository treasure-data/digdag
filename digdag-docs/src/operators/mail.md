# mail>: Sending email

**mail>** operator sends an email.

To use Gmail SMTP server, you need to do either of:

* a) Generate a new app password at [App passwords](https://security.google.com/settings/security/apppasswords). This needs to enable 2-Step Verification first.

* b) Enable access for less secure apps at [Less secure apps](https://www.google.com/settings/security/lesssecureapps). This works even if 2-Step Verification is not enabled.

    _export:
      mail:
        from: "you@gmail.com"

    +step1:
      mail>: body.txt
      subject: workflow started
      to: [me@example.com]

    +step2:
      mail>:
        data: this is email body embedded in a .dig file
      subject: workflow started
      to: [me@example.com]

    +step3:
      sh>: this_task_might_fail.sh
      _error:
        mail>: body.txt
        subject: this workflow failed
        to: [me@example.com]

## Secrets

* **mail.host**: HOST

  SMTP host name.

  Examples:

  ```
  mail.host: smtp.gmail.com
  ```

* **mail.port**: PORT

  SMTP port number.

  Examples:

  ```
  mail.port: 587
  ```

* **mail.username**: NAME

  SMTP login username.

  Examples:

  ```
  mail.username: me
  ```

* **mail.password**: PASSWORD

  SMTP login password.

  Examples:

  ```
  mail.password: MyPaSsWoRd
  ```

* **mail.tls**: BOOLEAN
  Enables TLS handshake.

  Examples:

  ```
  mail.tls: true
  ```

* **mail.ssl**: BOOLEAN

  Enables legacy SSL encryption.

  Examples:

  ```
  mail.ssl: false
  ```

## Options

* **mail>**: FILE

  Path to a mail body template file. This file can contain `${...}` syntax to embed variables.
  Alternatively, you can set `{data: TEXT}` to embed body text in the .dig file.

  Examples:

  ```
  mail>: mail_body.txt
  ```

  * or :command:`mail>: {body: Hello, this is from Digdag}`

* **subject**: SUBJECT

  Subject of the email.

  Examples:

  ```
  subject: Mail From Digdag
  ```

* **to**: [ADDR1, ADDR2, ...]

  To addresses.

  Examples:

  ```
  to: [analyst@examile.com]
  ```

* **from**: ADDR
  From address.

  Examples:

  ```
  from: admin@example.com
  ```

* **host**: NAME

  SMTP host name.

  Examples:

  ```
  host: smtp.gmail.com
  ```

* **port**: NAME

  SMTP port number.

  Examples:

  ```
  port: 587
  ```

* **username**: NAME

  SMTP login username.

  Examples:

  ```
  username: me
  ```

* **tls**: BOOLEAN

  Enables TLS handshake.

  Examples:

  ```
  tls: true
  ```

* **ssl**: BOOLEAN

  Enables legacy SSL encryption.

  Examples:

  ```
  ssl: false
  ```

* **html**: BOOLEAN

  Uses HTML mail (default: false).

  Examples:

  ```
  html: true
  ```

* **debug**: BOOLEAN

  Shows debug logs (default: false).

  Examples:

  ```
  debug: false
  ```

* **attach_files**: ARRAY

  Attach files. Each element is an object of:

  * Example: `path: FILE`: Path to a file to attach.

  * Example: `content_type`: Content-Type of this file. Default is application/octet-stream.

  * Example: `filename`: Name of this file. Default is base name of the path.

  Example:

      attach_files:
        - path: data.csv
        - path: output.dat
          filename: workflow_result_data.csv
        - path: images/image1.png
          content_type: image/png

