# VHS tape to record the demo gif.
# See https://github.com/charmbracelet/vhs
Output demo.gif

Require kafkactl

Set Shell "bash"
Set Width 1200
Set Height 600
Set Padding 30
Set FontSize 20
Set TypingSpeed 50ms

Hide
Type "export LESS='--chop-long-lines'" Enter
Ctrl+l
Show

Type "kafkactl --help | head -n 19" Enter
Sleep 7s

Ctrl+l
Set TypingSpeed 10ms
Type "# By default, information is shown in a tabular format" Sleep 200ms Enter

Set TypingSpeed 50ms
Type "kafkactl get brokers" Sleep 200ms  Enter
Sleep 3s

Ctrl+l
Set TypingSpeed 10ms
Type "# For scripting and additional output, try the JSON format" Sleep 200ms Enter

Set TypingSpeed 50ms
Type "kafkactl get topics --output=json | head -n 19" Sleep 200ms Enter
Sleep 5s

Ctrl+l
Set TypingSpeed 10ms
Type "# All commands come with --help and examples" Sleep 500ms Enter

Set TypingSpeed 50ms
Type "kafkactl consume --help | less" Sleep 1s Enter
Sleep 3s
Down Sleep 500ms
Down Sleep 500ms
Down Sleep 500ms
Down Sleep 500ms
Down Sleep 500ms
Down Sleep 500ms
Down Sleep 500ms
Down Sleep 500ms

Sleep 4s
