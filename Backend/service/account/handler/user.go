package handler

import (
	rPool "cloud_distributed_storage/Backend/cache/redis"
	"cloud_distributed_storage/Backend/common"
	cfg "cloud_distributed_storage/Backend/config"
	"cloud_distributed_storage/Backend/service/account/proto"
	dbcli "cloud_distributed_storage/Backend/service/dbproxy/client"
	"cloud_distributed_storage/Backend/util"
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type User struct{}

// GenToken : 生成token
func GenToken(username string) string {
	// 40位字符:md5(username+timestamp+token_salt)+timestamp[:8]
	ts := fmt.Sprintf("%x", time.Now().Unix())
	tokenPrefix := util.MD5([]byte(username + ts + "_tokensalt"))
	return tokenPrefix + ts[:8]
}

// Signup : RPC handler for user signup
func (u *User) Signup(ctx context.Context, req *proto.ReqSignup, res *proto.ResSignup) error {
	username := req.Username
	password := req.Password
	email := req.Email
	phone := req.Phone

	if len(username) < 2 || len(password) < 4 || len(email) == 0 {
		res.Code = common.StatusParamInvalid
		res.Message = "Invalid parameter"
		return errors.New("无效的参数")
	}
	enc_password := util.Sha1([]byte(password + cfg.Pwd_salt))
	dbResp, err := dbcli.UserSignup(username, enc_password, email, phone)

	if err != nil {
		res.Code = common.StatusRegisterFailed
		res.Message = "注册失败: " + err.Error()
		return err
	}

	if !dbResp.Suc {
		res.Code = common.StatusRegisterFailed
		res.Message = "注册失败: 数据库操作未成功"
		return errors.New("数据库操作未成功")
	}

	res.Code = common.StatusOK
	res.Message = "注册成功"
	return nil
}

// Login: RPC handler for user signin
func (u *User) Login(ctx context.Context, req *proto.ReqLogin, res *proto.ResLogin) error {

	username := req.Username
	password := req.Password
	encPassword := util.Sha1([]byte(password + cfg.Pwd_salt))

	dbResp, err := dbcli.UserLogin(username, encPassword)

	if err != nil || !dbResp.Suc {
		log.Println("err: ", err)
		res.Code = common.StatusLoginFailed
		return nil
	} else {
		token := GenToken(username)
		rConn := rPool.RedisPool().Get()
		defer rConn.Close()

		_, err := rConn.Do("SET", fmt.Sprintf("session_%s", token), username, "EX", 86400) // 24 hours expiration
		if err != nil {
			res.Code = common.StatusServerError
			return nil
		} else {
			res.Code = common.StatusOK
			res.Message = "LOGIN SUCCESS"
			res.Token = token
		}
	}
	return nil
}

// Logout: RPC handler for user logout
func (u *User) Logout(ctx context.Context, req *proto.ReqLogout, res *proto.ResLogout) error {
	token := req.Token

	rConn := rPool.RedisPool().Get()
	defer rConn.Close()

	_, err := rConn.Do("DEL", fmt.Sprintf("session_%s", token))
	if err != nil {
		res.Code = common.StatusServerError
		res.Message = "LOGOUT FAILED"
	} else {
		res.Code = common.StatusOK
		res.Message = "LOGOUT SUCCESS"
	}
	return nil
}

// DeleteAccount: RPC handler for account deletion
func (u *User) DeleteAccount(ctx context.Context, req *proto.ReqDeleteAccount, res *proto.ResDeleteAccount) error {
	username := req.Username
	password := req.Password

	encPassword := util.Sha1([]byte(password + cfg.Pwd_salt))
	pwdChecked, err := dbcli.UserLogin(username, encPassword)
	if err != nil || !pwdChecked.Suc {
		res.Code = common.StatusLoginFailed
		res.Message = "AUTHENTICATION FAILED"
		return nil
	}

	// Delete user data
	outRes, err := dbcli.UserLogout(username)
	if err != nil || !outRes.Suc {
		res.Code = common.StatusServerError
		res.Message = "FAILED TO DELETE USER DATA"
		return nil
	}

	// Delete user account
	delRes, err := dbcli.DeleteUserAccount(username)
	if err != nil || !delRes.Suc {
		res.Code = common.StatusServerError
		res.Message = "FAILED TO DELETE USER ACCOUNT"
		return nil
	}

	// Clear user session from Redis
	rConn := rPool.RedisPool().Get()
	defer rConn.Close()

	_, err = rConn.Do("DEL", fmt.Sprintf("session_%s", req.Token))
	if err != nil {
		// Log the error, but don't fail the operation
		fmt.Printf("Failed to clear user session: %v\n", err)
	}

	res.Code = common.StatusOK
	res.Message = "ACCOUNT DELETED SUCCESSFULLY"
	return nil
}

func (u *User) UserInfo(ctx context.Context, req *proto.ReqUserInfo, res *proto.ResUserInfo) error {

	username := req.Username

	dbResp, err := dbcli.GetUserInfo(req.Username)
	if err != nil {
		res.Code = common.StatusServerError
		res.Message = "服务错误"
		return nil
	}
	// 查不到对应的用户信息
	if !dbResp.Suc {
		res.Code = common.StatusUserNotExists
		res.Message = "用户不存在"
		return nil
	}

	user := dbcli.ToTableUser(dbResp.Data)

	// 3. 组装并且响应用户数据
	res.Code = common.StatusOK
	res.Username = username
	res.SignupAt = user.SignupAt
	res.LastActiveAt = user.LastActive
	res.Status = int32(user.Status)
	// TODO: 需增加接口支持完善用户信息(email/phone等)
	res.Email = user.Email
	res.Phone = user.Phone
	return nil
}
