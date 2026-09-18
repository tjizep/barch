#pragma once

struct lua_State;

/**
 * Install the `mail` table: send email over SMTP from a stored function.
 *
 *     local r = mail.send{to = "a@example.com", subject = "Hi", text = "Hello"}
 *     if not r.ok then error(r.error) end
 *
 * The server and its login come from the configuration space (`mail.server`,
 * `mail.user`, `mail.password` and friends), never from the script. Like
 * `http.request` the send suspends the calling function instead of blocking the
 * worker it is on. Nothing is installed when the build has no curl.
 */
void luaopen_mail(lua_State* L);
