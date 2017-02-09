# mail>: Sending email

**mail>** operator sends an email.

To use Gmail SMTP server, you need to do either of:

* a) Generate a new app password at `App passwords <https://security.google.com/settings/security/apppasswords>`_. This needs to enable 2-Step Verification first.

* b) Enable access for less secure apps at `Less secure apps <https://www.google.com/settings/security/lesssecureapps>`_. This works even if 2-Step Verification is not enabled.

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

* `mail.host: HOST`

  SMTP host name.

  * Example: `mail.host: smtp.gmail.com`

* `mail.port: PORT`

  SMTP port number.

  * Example: `mail.port: 587`

* `mail.username: NAME`

  SMTP login username.

  * Example: `mail.username: me`

* `mail.password: PASSWORD`

  SMTP login password.

  * Example: `mail.password: MyPaSsWoRd`

* `mail.tls: BOOLEAN`
  Enables TLS handshake.

  * Example: `mail.tls: true`

* `mail.ssl: BOOLEAN`

  Enables legacy SSL encryption.

  * Example: `mail.ssl: false`

## Options

* `mail>: FILE`

  Path to a mail body template file. This file can contain `${...}` syntax to embed variables.
  Alternatively, you can set `{data: TEXT}` to embed body text in the .dig file.

  * Example: `mail>: mail_body.txt`

  * or :command:`mail>: {body: Hello, this is from Digdag}`

* `subject: SUBJECT`

  Subject of the email.

  * Example: `subject: Mail From Digdag`

* `to: [ADDR1, ADDR2, ...]`

  To addresses.

  * Example: `to: [analyst@examile.com]`

* `from: ADDR`
  From address.

  * Example: `from: admin@example.com`

* `host: NAME`

  SMTP host name.

  * Example: `host: smtp.gmail.com`

* `port: NAME`

  SMTP port number.

  * Example: `port: 587`

* `username: NAME`

  SMTP login username.

  * Example: `username: me`

* `tls: BOOLEAN`

  Enables TLS handshake.

  * Example: `tls: true`

* `ssl: BOOLEAN`

  Enables legacy SSL encryption.

  * Example: `ssl: false`

* `html: BOOLEAN`

  Uses HTML mail (default: false).

  * Example: `html: true`

* `debug: BOOLEAN`

  Shows debug logs (default: false).

  * Example: `debug: false`

* `attach_files: ARRAY`

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

