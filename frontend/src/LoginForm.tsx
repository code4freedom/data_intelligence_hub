import { useState } from 'react'
import axios from 'axios'
import { Zap } from 'lucide-react'
import toast from 'react-hot-toast'

interface LoginFormProps {
    onLogin: (token: string) => void
}

export default function LoginForm({ onLogin }: LoginFormProps) {
    const [password, setPassword] = useState('')
    const [loading, setLoading] = useState(false)

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault()
        if (!password.trim()) return

        setLoading(true)
        try {
            const formData = new FormData()
            formData.append('username', 'admin') // OAuth2 spec requires username, even if unused
            formData.append('password', password)

            const res = await axios.post('/token', formData)
            const token = res.data.access_token
            onLogin(token)
            toast.success('Login successful')
        } catch (err: any) {
            toast.error(err?.response?.data?.detail || 'Login failed. Incorrect password.')
            setPassword('')
        } finally {
            setLoading(false)
        }
    }

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 flex items-center justify-center p-4">
            <div className="bg-white rounded-2xl shadow-xl w-full max-w-md overflow-hidden">
                <div className="p-8 text-center bg-slate-900">
                    <img src="/vcf-hub-logo.png" alt="VCF Hub" className="w-20 h-20 mx-auto object-contain mb-4" />
                    <h1 className="text-2xl font-bold text-white">VCF Intelligence Hub</h1>
                    <p className="text-slate-400 text-sm mt-1">Authentication Required</p>
                </div>

                <div className="p-8">
                    <form onSubmit={handleSubmit} className="space-y-6">
                        <div>
                            <label className="block text-sm font-medium text-slate-700 mb-2">
                                Application Password
                            </label>
                            <input
                                type="password"
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                className="w-full border-2 border-slate-200 rounded-lg px-4 py-3 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-colors"
                                placeholder="Enter password..."
                                autoFocus
                                disabled={loading}
                            />
                        </div>

                        <button
                            type="submit"
                            disabled={loading || !password.trim()}
                            className="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-medium py-3 px-4 rounded-lg transition-colors flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                            {loading ? 'Authenticating...' : (
                                <>
                                    <Zap className="w-5 h-5" />
                                    Access Dashboard
                                </>
                            )}
                        </button>
                    </form>
                </div>
            </div>
        </div>
    )
}
