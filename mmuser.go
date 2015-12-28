package irckit

import (
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jpillora/backoff"
	"github.com/mattermost/platform/model"
	"github.com/sorcix/irc"
)

func NewUserMM(c net.Conn, srv Server, cfg *MmCfg) *User {
	u := NewUser(&conn{
		Conn:    c,
		Encoder: irc.NewEncoder(c),
		Decoder: irc.NewDecoder(c),
	})
	u.Srv = srv
	u.MmInfo.Cfg = cfg

	if cfg.LoginServiceNick == nil {
		cfg.LoginServiceNick = "mattermost"
	}
	// used for login
	mattermostService := &User{Nick: cfg.LoginServiceNick, User: "mattermost", Real: "loginservice", Host: "service", channels: map[Channel]struct{}{}}
	mattermostService.MmGhostUser = true
	srv.Add(mattermostService)
	if _, ok := srv.HasUser("mattermost"); !ok {
		go srv.Handle(mattermostService)
	}

	return u
}

func (u *User) loginToMattermost() error {
	b := &backoff.Backoff{
		Min:    time.Second,
		Max:    5 * time.Minute,
		Jitter: true,
	}
	// login to mattermost
	//u.Credentials = &MmCredentials{Server: url, Team: team, Login: email, Pass: pass}
	MmClient := model.NewClient("https://" + u.Credentials.Server)
	var myinfo *model.Result
	var appErr *model.AppError
	for {
		logger.Debug("retrying login", u.Credentials.Team, u.Credentials.Login, u.Credentials.Server)
		myinfo, appErr = MmClient.LoginByEmail(u.Credentials.Team, u.Credentials.Login, u.Credentials.Pass)
		if appErr != nil {
			d := b.Duration()
			if !strings.Contains(appErr.DetailedError, "connection refused") &&
				!strings.Contains(appErr.DetailedError, "invalid character") {
				return errors.New(appErr.Message)
			}
			logger.Infof("LOGIN: %s, reconnecting in %s", appErr, d)
			time.Sleep(d)
			continue
		}
		break
	}
	// reset timer
	b.Reset()
	u.MmUser = myinfo.Data.(*model.User)

	myinfo, _ = MmClient.GetMyTeam("")
	u.MmTeam = myinfo.Data.(*model.Team)

	// setup websocket connection
	wsurl := "wss://" + u.Credentials.Server + "/api/v1/websocket"
	header := http.Header{}
	header.Set(model.HEADER_AUTH, "BEARER "+MmClient.AuthToken)

	var WsClient *websocket.Conn
	var err error
	for {
		WsClient, _, err = websocket.DefaultDialer.Dial(wsurl, header)
		if err != nil {
			d := b.Duration()
			logger.Infof("WSS: %s, reconnecting in %s", err, d)
			time.Sleep(d)
			continue
		}
		break
	}
	b.Reset()

	u.MmClient = MmClient
	u.MmWsClient = WsClient

	// populating users
	u.updateMMUsers()

	// populating channels
	u.updateMMChannels()

	// fetch users and channels from mattermost
	u.addUsersToChannels()

	return nil
}

func (u *User) createMMUser(mmuser *model.User) *User {
	if ghost, ok := u.Srv.HasUser(mmuser.Username); ok {
		return ghost
	}
	ghost := &User{Nick: mmuser.Username, User: mmuser.Id, Real: mmuser.FirstName + " " + mmuser.LastName, Host: u.MmClient.Url, channels: map[Channel]struct{}{}}
	ghost.MmGhostUser = true
	return ghost
}

func (u *User) addUsersToChannels() {
	var mmConnected bool
	srv := u.Srv
	// already connected to a mm server ? add teamname as suffix
	if _, ok := srv.HasChannel("#town-square"); ok {
		//mmConnected = true
	}
	rate := time.Second / 1
	throttle := time.Tick(rate)

	for _, mmchannel := range u.MmChannels.Channels {

		// exclude direct messages
		if strings.Contains(mmchannel.Name, "__") {
			continue
		}
		<-throttle
		go func(mmchannel *model.Channel) {
			edata, _ := u.MmClient.GetChannelExtraInfo(mmchannel.Id, "")
			if mmConnected {
				mmchannel.Name = mmchannel.Name + "-" + u.MmTeam.Name
			}

			// join ourself to all channels
			ch := srv.Channel("#" + mmchannel.Name)
			ch.Topic(u, u.getMMChannelHeader(mmchannel.Id))
			ch.Join(u)

			// add everyone on the MM channel to the IRC channel
			for _, d := range edata.Data.(*model.ChannelExtra).Members {
				if mmConnected {
					d.Username = d.Username + "-" + u.MmTeam.Name
				}
				// already joined
				if d.Id == u.MmUser.Id {
					continue
				}

				cghost, ok := srv.HasUser(d.Username)
				if !ok {
					ghost := u.createMMUser(u.MmUsers[d.Id])
					ghost.MmGhostUser = true
					logger.Info("adding", ghost.Nick, "to #"+mmchannel.Name)
					srv.Add(ghost)
					go srv.Handle(ghost)
					ch := srv.Channel("#" + mmchannel.Name)
					ch.Join(ghost)
				} else {
					ch := srv.Channel("#" + mmchannel.Name)
					ch.Join(cghost)
				}
			}

			// post everything to the channel you haven't seen yet
			postlist := u.getMMPostsSince(mmchannel.Id, u.MmChannels.Members[mmchannel.Id].LastViewedAt)
			if postlist == nil {
				logger.Errorf("something wrong with getMMPostsSince")
				return
			}
			logger.Debugf("%#v", u.MmChannels.Members[mmchannel.Id])
			// traverse the order in reverse
			for i := len(postlist.Order) - 1; i >= 0; i-- {
				for _, post := range strings.Split(postlist.Posts[postlist.Order[i]].Message, "\n") {
					ch.SpoofMessage(u.MmUsers[postlist.Posts[postlist.Order[i]].UserId].Username, post)
				}
			}
			u.updateMMLastViewed(mmchannel.Id)

		}(mmchannel)
	}

	// add all users, also who are not on channels
	for _, mmuser := range u.MmUsers {
		// do not add our own nick
		if mmuser.Id == u.MmUser.Id {
			continue
		}
		_, ok := srv.HasUser(mmuser.Username)
		if !ok {
			if mmConnected {
				mmuser.Username = mmuser.Username + "-" + u.MmTeam.Name
			}
			ghost := u.createMMUser(mmuser)
			ghost.MmGhostUser = true
			logger.Info("adding", ghost.Nick, "without a channel")
			srv.Add(ghost)
			go srv.Handle(ghost)
		}
	}
}

type MmInfo struct {
	MmGhostUser    bool
	MmClient       *model.Client
	MmWsClient     *websocket.Conn
	Srv            Server
	MmUsers        map[string]*model.User
	MmUser         *model.User
	MmChannels     *model.ChannelList
	MmMoreChannels *model.ChannelList
	MmTeam         *model.Team
	Credentials    *MmCredentials
	Cfg            *MmCfg
}

type MmCredentials struct {
	Login  string
	Team   string
	Pass   string
	Server string
}

type MmCfg struct {
	AllowedServers   []string
	DefaultServer    string
	DefaultTeam      string
	LoginServiceNick string
}

func (u *User) WsReceiver() {
	var rmsg model.Message
	for {
		if err := u.MmWsClient.ReadJSON(&rmsg); err != nil {
			logger.Critical(err)
			// did the user quit
			if _, ok := u.Srv.HasUser(u.Nick); !ok {
				logger.Debug("user has quit, not reconnecting")
				u.MmWsClient.Close()
				return
			}
			// reconnect
			u.loginToMattermost()
		}
		logger.Debugf("WsReceiver: %#v", rmsg)
		switch rmsg.Action {
		case model.ACTION_POSTED:
			u.handleWsActionPost(&rmsg)
		case model.ACTION_USER_REMOVED:
			u.handleWsActionUserRemoved(&rmsg)
		case model.ACTION_USER_ADDED:
			u.handleWsActionUserAdded(&rmsg)
		}
	}
}

func (u *User) handleWsActionPost(rmsg *model.Message) {
	data := model.PostFromJson(strings.NewReader(rmsg.Props["post"]))
	logger.Debug("receiving userid", data.UserId)
	if data.UserId == u.MmUser.Id {
		// our own message - http://www.fileformat.info/info/unicode/char/180e/fontsupport.htm
		if strings.Contains(data.Message, "᠎") {
			logger.Debugf("message is sent from IRC, contains unicode, not relaying", data.Message)
			return
		}
	}
	// we don't have the user, refresh the userlist
	if u.MmUsers[data.UserId] == nil {
		u.updateMMUsers()
	}
	ghost := u.createMMUser(u.MmUsers[data.UserId])
	// our own message, set our IRC self as user, not our mattermost self
	if data.UserId == u.MmUser.Id {
		ghost = u
	}
	rcvchannel := u.getMMChannelName(data.ChannelId)
	// direct message
	if strings.Contains(rcvchannel, "__") {
		// our own message, ignore because we can't handle/fake those on IRC
		if data.UserId == u.MmUser.Id {
			return
		}
		logger.Debug("direct message")
		var rcvuser string
		rcvusers := strings.Split(rcvchannel, "__")
		if rcvusers[0] != u.MmUser.Id {
			rcvuser = u.MmUsers[rcvusers[0]].Username
		} else {
			rcvuser = u.MmUsers[rcvusers[1]].Username
		}
		msgs := strings.Split(data.Message, "\n")
		for _, m := range msgs {
			u.MsgSpoofUser(rcvuser, m)
		}
		return
	}

	logger.Debugf("channel id %#v, name %#v", data.ChannelId, u.getMMChannelName(data.ChannelId))
	ch := u.Srv.Channel("#" + rcvchannel)

	// join if not in channel
	if !ch.HasUser(ghost) {
		ch.Join(ghost)
	}
	msgs := strings.Split(data.Message, "\n")
	for _, m := range msgs {
		ch.Message(ghost, m)
	}

	if len(data.Filenames) > 0 {
		logger.Debugf("files detected")
		for _, fname := range data.Filenames {
			logger.Debug("filename: ", fname)
			ch.Message(ghost, "download file - https://"+u.Credentials.Server+"/api/v1/files/get"+fname)
		}
	}
	logger.Debug(u.MmUsers[data.UserId].Username, ":", data.Message)
	logger.Debugf("%#v", data)

	// updatelastviewed
	u.updateMMLastViewed(data.ChannelId)
	return
}

func (u *User) handleWsActionUserRemoved(rmsg *model.Message) {
	if u.MmUsers[rmsg.UserId] == nil {
		u.updateMMUsers()
	}
	ch := u.Srv.Channel("#" + u.getMMChannelName(rmsg.ChannelId))

	// remove ourselves from the channel
	if rmsg.UserId == u.MmUser.Id {
		ch.Part(u, "")
		return
	}

	ghost := u.createMMUser(u.MmUsers[rmsg.UserId])
	if ghost == nil {
		logger.Debug("couldn't remove user", rmsg.UserId, u.MmUsers[rmsg.UserId].Username)
		return
	}
	ch.Part(ghost, "")
	return
}

func (u *User) handleWsActionUserAdded(rmsg *model.Message) {
	if u.getMMChannelName(rmsg.ChannelId) == "" {
		u.updateMMChannels()
	}

	if u.MmUsers[rmsg.UserId] == nil {
		u.updateMMUsers()
	}

	ch := u.Srv.Channel("#" + u.getMMChannelName(rmsg.ChannelId))
	// add ourselves to the channel
	if rmsg.UserId == u.MmUser.Id {
		logger.Debug("ACTION_USER_ADDED adding myself to", u.getMMChannelName(rmsg.ChannelId), rmsg.ChannelId)
		ch.Topic(u, u.getMMChannelHeader(rmsg.ChannelId))
		ch.Join(u)
		return
	}
	ghost := u.createMMUser(u.MmUsers[rmsg.UserId])
	if ghost == nil {
		logger.Debug("couldn't add user", rmsg.UserId, u.MmUsers[rmsg.UserId].Username)
		return
	}
	ch.Join(ghost)
	return
}

func (u *User) getMMChannelHeader(id string) string {
	for _, channel := range append(u.MmChannels.Channels, u.MmMoreChannels.Channels...) {
		if channel.Id == id {
			return channel.Header
		}
	}
	return ""
}

func (u *User) getMMChannelName(id string) string {
	for _, channel := range append(u.MmChannels.Channels, u.MmMoreChannels.Channels...) {
		if channel.Id == id {
			return channel.Name
		}
	}
	// not found? could be a new direct message from mattermost. Try to update and check again
	u.updateMMChannels()
	for _, channel := range append(u.MmChannels.Channels, u.MmMoreChannels.Channels...) {
		if channel.Id == id {
			return channel.Name
		}
	}
	return ""
}

func (u *User) getMMChannelId(name string) string {
	for _, channel := range append(u.MmChannels.Channels, u.MmMoreChannels.Channels...) {
		if channel.Name == name {
			return channel.Id
		}
	}
	return ""
}

func (u *User) getMMUserId(name string) string {
	for id, u := range u.MmUsers {
		if u.Username == name {
			return id
		}
	}
	return ""
}

func (u *User) MsgUser(toUser *User, msg string) {
	u.Encode(&irc.Message{
		Prefix:   toUser.Prefix(),
		Command:  irc.PRIVMSG,
		Params:   []string{u.Nick},
		Trailing: msg,
	})
}

func (u *User) MsgSpoofUser(rcvuser string, msg string) {
	u.Encode(&irc.Message{
		Prefix:   &irc.Prefix{Name: rcvuser, User: rcvuser, Host: rcvuser},
		Command:  irc.PRIVMSG,
		Params:   []string{u.Nick},
		Trailing: msg,
	})
}

func (u *User) handleMMDM(toUser *User, msg string) {
	var channel string
	// We don't have a DM with this user yet.
	if u.getMMChannelId(toUser.User+"__"+u.MmUser.Id) == "" && u.getMMChannelId(u.MmUser.Id+"__"+toUser.User) == "" {
		// create DM channel
		_, err := u.MmClient.CreateDirectChannel(map[string]string{"user_id": toUser.User})
		if err != nil {
			logger.Debugf("direct message to %#v failed: %s", toUser, err)
		}
		// update our channels
		mmchannels, _ := u.MmClient.GetChannels("")
		u.MmChannels = mmchannels.Data.(*model.ChannelList)
	}

	// build the channel name
	if toUser.User > u.MmUser.Id {
		channel = u.MmUser.Id + "__" + toUser.User
	} else {
		channel = toUser.User + "__" + u.MmUser.Id
	}
	// build & send the message
	msg = strings.Replace(msg, "\r", "", -1)
	post := &model.Post{ChannelId: u.getMMChannelId(channel), Message: msg}
	u.MmClient.CreatePost(post)
}

func (u *User) handleMMServiceBot(toUser *User, msg string) {
	commands := strings.Fields(msg)
	switch commands[0] {
	case "LOGIN", "login":
		{
			cred := &MmCredentials{}
			datalen := 5
			if u.Cfg.DefaultTeam != "" {
				cred.Team = u.Cfg.DefaultTeam
				datalen--
			}
			if u.Cfg.DefaultServer != "" {
				cred.Server = u.Cfg.DefaultServer
				datalen--
			}
			data := strings.Split(msg, " ")
			if len(data) == datalen {
				cred.Pass = data[len(data)-1]
				cred.Login = data[len(data)-2]
				// no default server or team specified
				if cred.Server == "" && cred.Team == "" {
					cred.Server = data[len(data)-4]
				}
				if cred.Team == "" {
					cred.Team = data[len(data)-3]
				}
				if cred.Server == "" {
					cred.Server = data[len(data)-3]
				}

			}

			// incorrect arguments
			if len(data) != datalen {
				// no server or team
				if cred.Team != "" && cred.Server != "" {
					u.MsgUser(toUser, "need LOGIN <login> <pass>")
					return
				}
				// server missing
				if cred.Team != "" {
					u.MsgUser(toUser, "need LOGIN <server> <login> <pass>")
					return
				}
				// team missing
				if cred.Server != "" {
					u.MsgUser(toUser, "need LOGIN <team> <login> <pass>")
					return
				}
				u.MsgUser(toUser, "need LOGIN <server> <team> <login> <pass>")
				return
			}

			if !u.isValidMMServer(cred.Server) {
				u.MsgUser(toUser, "not allowed to connect to "+cred.Server)
				return
			}

			u.Credentials = cred
			err := u.loginToMattermost()
			if err != nil {
				u.MsgUser(toUser, err.Error())
				return
			}
			go u.WsReceiver()
			u.MsgUser(toUser, "login OK")

		}
	case "SEARCH", "search":
		{
			postlist := u.searchMMPosts(strings.Join(commands[1:], " "))
			if postlist == nil || len(postlist.Order) == 0 {
				u.MsgUser(toUser, "no results")
				return
			}
			for i := len(postlist.Order) - 1; i >= 0; i-- {
				timestamp := time.Unix(postlist.Posts[postlist.Order[i]].CreateAt/1000, 0).Format("January 02, 2006 15:04")
				channelname := u.getMMChannelName(postlist.Posts[postlist.Order[i]].ChannelId)
				u.MsgUser(toUser, "#"+channelname+" <"+u.MmUsers[postlist.Posts[postlist.Order[i]].UserId].Username+"> "+timestamp)
				u.MsgUser(toUser, strings.Repeat("=", len("#"+channelname+" <"+u.MmUsers[postlist.Posts[postlist.Order[i]].UserId].Username+"> "+timestamp)))
				for _, post := range strings.Split(postlist.Posts[postlist.Order[i]].Message, "\n") {
					u.MsgUser(toUser, post)
				}
				u.MsgUser(toUser, "")
				u.MsgUser(toUser, "")
			}
		}
	case "SCROLLBACK", "scrollback", "sb":
		{
			if len(commands) != 3 {
				u.MsgUser(toUser, "need SCROLLBACK <channel> <lines>")
				u.MsgUser(toUser, "e.g. SCROLLBACK #bugs 10 (show last 10 lines from #bugs)")
				return
			}
			limit, err := strconv.Atoi(commands[2])
			if err != nil {
				u.MsgUser(toUser, "need SCROLLBACK <channel> <lines>")
				u.MsgUser(toUser, "e.g. SCROLLBACK #bugs 10 (show last 10 lines from #bugs)")
				return
			}
			if !strings.Contains(commands[1], "#") {
				u.MsgUser(toUser, "need SCROLLBACK <channel> <lines>")
				u.MsgUser(toUser, "e.g. SCROLLBACK #bugs 10 (show last 10 lines from #bugs)")
				return
			}
			commands[1] = strings.Replace(commands[1], "#", "", -1)
			postlist := u.getMMPosts(u.getMMChannelId(commands[1]), limit)
			if postlist == nil || len(postlist.Order) == 0 {
				u.MsgUser(toUser, "no results")
				return
			}
			for i := len(postlist.Order) - 1; i >= 0; i-- {
				nick := u.MmUsers[postlist.Posts[postlist.Order[i]].UserId].Username
				for _, post := range strings.Split(postlist.Posts[postlist.Order[i]].Message, "\n") {
					u.MsgUser(toUser, "<"+nick+"> "+post)
				}
			}
		}
	default:
		u.MsgUser(toUser, "possible commands: LOGIN, SEARCH, SCROLLBACK")
		u.MsgUser(toUser, "<command> help for more info")
	}

}

func (u *User) syncMMChannel(id string, name string) {
	var mmConnected bool
	srv := u.Srv
	// already connected to a mm server ? add teamname as suffix
	if _, ok := srv.HasChannel("#town-square"); ok {
		//mmConnected = true
	}

	edata, _ := u.MmClient.GetChannelExtraInfo(id, "")
	for _, d := range edata.Data.(*model.ChannelExtra).Members {
		if mmConnected {
			d.Username = d.Username + "-" + u.MmTeam.Name
		}
		// join all the channels we're on on MM
		if d.Id == u.MmUser.Id {
			ch := srv.Channel("#" + name)
			logger.Debug("syncMMChannel adding myself to ", name, id)
			ch.Topic(u, u.getMMChannelHeader(id))
			ch.Join(u)
		}

		cghost, ok := srv.HasUser(d.Username)
		if !ok {
			ghost := u.createMMUser(u.MmUsers[d.Id])
			ghost.MmGhostUser = true
			logger.Info("adding", ghost.Nick, "to #"+name)
			srv.Add(ghost)
			go srv.Handle(ghost)
			ch := srv.Channel("#" + name)
			ch.Join(ghost)
		} else {
			ch := srv.Channel("#" + name)
			ch.Join(cghost)
		}
	}
}

func (u *User) joinMMChannel(channel string) error {
	if u.getMMChannelId(strings.Replace(channel, "#", "", 1)) == "" {
		return errors.New("failed to join")
	}
	_, err := u.MmClient.JoinChannel(u.getMMChannelId(strings.Replace(channel, "#", "", 1)))
	if err != nil {
		return errors.New("failed to join")
	}
	u.syncMMChannel(u.getMMChannelId(strings.Replace(channel, "#", "", 1)), strings.Replace(channel, "#", "", 1))
	return nil
}

func (u *User) getMMPostsSince(channelId string, time int64) *model.PostList {
	res, err := u.MmClient.GetPostsSince(channelId, time)
	if err != nil {
		return nil
	}
	return res.Data.(*model.PostList)
}

func (u *User) searchMMPosts(query string) *model.PostList {
	res, err := u.MmClient.SearchPosts(query)
	if err != nil {
		return nil
	}
	return res.Data.(*model.PostList)
}

func (u *User) getMMPosts(channelId string, limit int) *model.PostList {
	res, err := u.MmClient.GetPosts(channelId, 0, limit, "")
	if err != nil {
		return nil
	}
	return res.Data.(*model.PostList)
}

func (u *User) updateMMChannelHeader(channelId string, header string) {
	data := make(map[string]string)
	data["channel_id"] = channelId
	data["channel_header"] = header
	logger.Debugf("updating channelheader %#v, %#v", channelId, header)
	_, err := u.MmClient.UpdateChannelHeader(data)
	if err != nil {
		logger.Info(err)
	}
}

func (u *User) updateMMLastViewed(channelId string) {
	logger.Debugf("posting lastview %#v", channelId)
	_, err := u.MmClient.UpdateLastViewedAt(channelId)
	if err != nil {
		logger.Info(err)
	}
}

func (u *User) updateMMChannels() error {
	mmchannels, _ := u.MmClient.GetChannels("")
	u.MmChannels = mmchannels.Data.(*model.ChannelList)
	mmchannels, _ = u.MmClient.GetMoreChannels("")
	u.MmMoreChannels = mmchannels.Data.(*model.ChannelList)
	return nil
}

func (u *User) updateMMUsers() error {
	mmusers, _ := u.MmClient.GetProfiles(u.MmUser.TeamId, "")
	u.MmUsers = mmusers.Data.(map[string]*model.User)
	return nil
}

func (u *User) isValidMMServer(server string) bool {
	if len(u.Cfg.AllowedServers) > 0 {
		logger.Debug("allowedservers:", u.Cfg.AllowedServers)
		for _, srv := range u.Cfg.AllowedServers {
			if srv == server {
				return true
			}
		}
		return false
	}
	return true
}
