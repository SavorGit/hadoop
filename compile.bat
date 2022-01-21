@Echo Off & color 07

Title %CD%

call mvn clean compile -Dmaven.test.skip=true 

Echo.
Echo.
Echo ^>^>^>^>^>^>^>^>^>^> Execute '%CD%' done!!! ^<^<^<^<^<^<^<^<^<^<
Echo.

Pause