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
        cc: [foo@example.com,bar@example.com]

      +step2:
        mail>:
          data: this is email body embedded in a .dig file
        subject: workflow started
        to: [me@example.com]
        bcc: [foo@example.com,bar@example.com]

      +step3:
        sh>: this_task_might_fail.sh
        _error:
          mail>: body.txt
          subject: this workflow failed
          to: [me@example.com]

## Secrets

When you set those parameters, use [digdag secrets command](https://docs.digdag.io/command_reference.html#secrets).

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
  
  If you encountered `Unsecure 'password' parameter is deprecated.` message, please confirm you used the following command.
  
  ```
  # Server Mode
  digdag secret --project test_mail --set mail.password=xxxxx
  
  # Local Mode stores the parameter into `~/.config/digdag/secrets/mail.password`
  digdag secret --local --set mail.password=xxxxx
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

  * or :command:`mail>: {data: "Hello, this is from Digdag"}`

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

* **cc**: [ADDR1, ADDR2, ...]

  Cc addresses.

  Examples:

  ```
  cc: [analyst@examile.com]
  ```

* **bcc**: [ADDR1, ADDR2, ...]

  Bcc addresses.

  Examples:

  ```
  bcc: [analyst@examile.com]
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

  * **path: FILE**: Path to a file to attach.

  * **content_type: STRING**: Content-Type of this file. Default is application/octet-stream.

  * **filename: NAME**: Name of this file. Default is base name of the path.

  Example:

      attach_files:
        - path: data.csv
        - path: output.dat
          filename: workflow_result_data.csv
        - path: images/image1.png
          content_type: image/png

